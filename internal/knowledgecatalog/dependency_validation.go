package knowledgecatalog

import (
	"fmt"
	"sort"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"gorm.io/gorm"
)

const (
	maximumDependencyGraphNodes = 256
	maximumDependencyGraphEdges = 1024
	maximumDependencyGraphDepth = 16

	maximumDependencyExpressionWalkNodes = 4096
	maximumDependencyExpressionWalkDepth = 64
)

type dependencyStage uint8

const (
	dependencyStageInvalid dependencyStage = iota
	dependencyStageExtraction
	dependencyStageAlias
	dependencyStageCalculated
)

type dependencyDefinitionSemantics struct {
	stage        dependencyStage
	inputFields  []string
	outputFields []string
}

type dependencyVersionKey struct {
	objectID string
	version  int64
}

type dependencyValidationNode struct {
	version          versionRecord
	records          []dependencyRecord
	semantics        dependencyDefinitionSemantics
	semanticsLoaded  bool
	semanticsLoadErr error
}

type dependencySemanticValidator struct {
	database *gorm.DB
	tenantID string
	nodes    map[dependencyVersionKey]*dependencyValidationNode
	// Successful semantic validation is intrinsic to an immutable node and its
	// immutable outgoing edges, so shared DAG tails are checked once per request.
	// Shape walks remain per-root to preserve their exact node/edge/depth limits.
	semanticsValidated map[dependencyVersionKey]bool
	budget             dependencySemanticWorkBudget
	work               dependencySemanticWork
}

type dependencySemanticWorkBudget struct {
	nodes           int64
	edges           int64
	definitionBytes int64
	queries         int64
}

type dependencySemanticWork struct {
	nodes           int64
	edges           int64
	definitionBytes int64
	queries         int64
}

type dependencyGraphWalk struct {
	validator *dependencySemanticValidator
	states    map[dependencyVersionKey]uint8
	depths    map[dependencyVersionKey]int
	nodes     int
	edges     int
}

// validateDecodedVersionDependencies validates the graph rooted at one
// already-authorized definition. Trusted dependency scalars are walked before
// any target body is decoded, so a forbidden, cyclic, or oversized graph does
// not turn target definitions into a read oracle. Inactive definitions are
// deliberately structural-only: drafts are an authoring surface, and disabled
// or deleted versions may retain a body that has not passed publication
// semantics. No inactive body is executable.
func validateDecodedVersionDependencies(
	database *gorm.DB,
	version versionRecord,
	decoded decodedDefinition,
	records []dependencyRecord,
) error {
	validator := newDependencySemanticValidator(database, version.TenantID, dependencySemanticWorkBudget{
		nodes: maximumDependencyGraphNodes,
		// One newly opened node may expose its complete sealed direct set
		// before the per-root walker reaches edge 1,025. Leave exactly one
		// direct-set margin so the deterministic graph-corruption error wins.
		edges:           maximumDependencyGraphEdges + maximumDependenciesPerVersion,
		definitionBytes: int64(maximumDependencyGraphNodes) * maximumDefinitionBytes,
		queries:         int64(maximumDependencyGraphNodes) * 8,
	})
	return validator.validateDecodedVersionDependencies(version, decoded, records)
}

func newDependencySemanticValidator(
	database *gorm.DB,
	tenantID string,
	budget dependencySemanticWorkBudget,
) *dependencySemanticValidator {
	return &dependencySemanticValidator{
		database:           database,
		tenantID:           tenantID,
		nodes:              make(map[dependencyVersionKey]*dependencyValidationNode),
		semanticsValidated: make(map[dependencyVersionKey]bool),
		budget:             budget,
	}
}

func (validator *dependencySemanticValidator) validateDecodedVersionDependencies(
	version versionRecord,
	decoded decodedDefinition,
	records []dependencyRecord,
) error {
	switch version.State {
	case StateDraft, StateDisabled, StateDeleted:
	case StateActive:
		if !decoded.ObjectTypeKnown {
			return fmt.Errorf("%w: active definition semantics are unavailable", ErrCorrupt)
		}
	default:
		return fmt.Errorf("%w: dependency semantic root state is invalid", ErrCorrupt)
	}
	if err := validator.seedDecodedVersion(version, decoded, records); err != nil {
		return err
	}
	rootKey := dependencyVersionKey{objectID: version.KnowledgeObjectID, version: version.ObjectVersion}
	walk := &dependencyGraphWalk{
		validator: validator,
		states:    make(map[dependencyVersionKey]uint8),
		depths:    make(map[dependencyVersionKey]int),
	}
	depth, err := walk.validateGraphShape(rootKey)
	if err != nil {
		return err
	}
	if depth > maximumDependencyGraphDepth {
		return fmt.Errorf(
			"%w: dependency graph depth exceeds %d",
			ErrCorrupt,
			maximumDependencyGraphDepth,
		)
	}
	if version.State != StateActive {
		return nil
	}
	return validator.validateGraphSemantics(rootKey)
}

func (validator *dependencySemanticValidator) seedDecodedVersion(
	version versionRecord,
	decoded decodedDefinition,
	records []dependencyRecord,
) error {
	if version.State != StateActive {
		return validator.seedStructuralNode(version, records)
	}
	semantics, err := dependencySemanticsFromDefinition(decoded.Definition)
	if err != nil {
		return fmt.Errorf("%w: decoded dependency semantics are invalid: %v", ErrCorrupt, err)
	}
	return validator.seedSemanticNode(version, records, semantics)
}

func (validator *dependencySemanticValidator) seedStructuralNode(
	version versionRecord,
	records []dependencyRecord,
) error {
	key := dependencyVersionKey{objectID: version.KnowledgeObjectID, version: version.ObjectVersion}
	if existing, found := validator.nodes[key]; found {
		if existing.version.TenantID != version.TenantID ||
			existing.version.KnowledgeObjectID != version.KnowledgeObjectID ||
			existing.version.ObjectVersion != version.ObjectVersion ||
			len(existing.records) != len(records) {
			return fmt.Errorf("%w: dependency semantic cache identity disagrees", ErrCorrupt)
		}
		for index := range records {
			if existing.records[index] != records[index] {
				return fmt.Errorf("%w: dependency semantic cache rows disagree", ErrCorrupt)
			}
		}
		return nil
	}
	validator.nodes[key] = &dependencyValidationNode{version: version, records: records}
	validator.work.nodes++
	validator.work.edges += int64(len(records))
	return validator.validateAggregateWork()
}

func (validator *dependencySemanticValidator) seedSemanticNode(
	version versionRecord,
	records []dependencyRecord,
	semantics dependencyDefinitionSemantics,
) error {
	if err := validator.seedStructuralNode(version, records); err != nil {
		return err
	}
	key := dependencyVersionKey{objectID: version.KnowledgeObjectID, version: version.ObjectVersion}
	node := validator.nodes[key]
	if node.version.TenantID != version.TenantID || node.version.KnowledgeObjectID != version.KnowledgeObjectID ||
		node.version.ObjectVersion != version.ObjectVersion {
		return fmt.Errorf("%w: dependency semantic cache identity disagrees", ErrCorrupt)
	}
	node.version = version
	node.records = records
	node.semantics = semantics
	node.semanticsLoaded = true
	node.semanticsLoadErr = nil
	return nil
}

func (validator *dependencySemanticValidator) validateAggregateWork() error {
	if validator.work.nodes > validator.budget.nodes {
		return fmt.Errorf("%w: aggregate dependency semantic node budget exceeded", control.ErrCapacityExceeded)
	}
	if validator.work.edges > validator.budget.edges {
		return fmt.Errorf("%w: aggregate dependency semantic edge budget exceeded", control.ErrCapacityExceeded)
	}
	if validator.work.definitionBytes > validator.budget.definitionBytes {
		return fmt.Errorf("%w: aggregate dependency semantic body budget exceeded", control.ErrCapacityExceeded)
	}
	if validator.work.queries > validator.budget.queries {
		return fmt.Errorf("%w: aggregate dependency semantic query budget exceeded", control.ErrCapacityExceeded)
	}
	return nil
}

// validateGraphShape returns the longest number of edges below key. It reads
// only trusted scalar/version/dependency authorities; definition blobs are
// intentionally deferred to validateGraphSemantics.
func (walk *dependencyGraphWalk) validateGraphShape(
	key dependencyVersionKey,
) (int, error) {
	switch walk.states[key] {
	case 1:
		return 0, fmt.Errorf("%w: dependency graph contains a cycle", ErrCorrupt)
	case 2:
		return walk.depths[key], nil
	}
	walk.nodes++
	if walk.nodes > maximumDependencyGraphNodes {
		return 0, fmt.Errorf(
			"%w: dependency graph nodes exceed %d",
			ErrCorrupt,
			maximumDependencyGraphNodes,
		)
	}
	node, err := walk.validator.loadStructuralNode(key)
	if err != nil {
		return 0, err
	}
	walk.states[key] = 1
	longest := 0
	for _, record := range node.records {
		walk.edges++
		if walk.edges > maximumDependencyGraphEdges {
			return 0, fmt.Errorf(
				"%w: dependency graph edges exceed %d",
				ErrCorrupt,
				maximumDependencyGraphEdges,
			)
		}
		targetKey := dependencyVersionKey{
			objectID: record.TargetObjectID,
			version:  record.TargetObjectVersion,
		}
		targetDepth, targetErr := walk.validateGraphShape(targetKey)
		if targetErr != nil {
			return 0, targetErr
		}
		if targetDepth >= maximumDependencyGraphDepth {
			return 0, fmt.Errorf(
				"%w: dependency graph depth exceeds %d",
				ErrCorrupt,
				maximumDependencyGraphDepth,
			)
		}
		longest = max(longest, targetDepth+1)
	}
	walk.states[key] = 2
	walk.depths[key] = longest
	return longest, nil
}

func (validator *dependencySemanticValidator) loadStructuralNode(
	key dependencyVersionKey,
) (*dependencyValidationNode, error) {
	if node, found := validator.nodes[key]; found {
		return node, nil
	}
	validator.work.queries += 6
	if err := validator.validateAggregateWork(); err != nil {
		return nil, err
	}
	version, found, err := readVersionRecord(
		validator.database,
		validator.tenantID,
		key.objectID,
		key.version,
	)
	if err != nil {
		return nil, err
	}
	if !found || validateVersion(version) != nil {
		return nil, fmt.Errorf("%w: dependency target immutable version is invalid", ErrCorrupt)
	}
	if validator.work.edges > validator.budget.edges-version.DependencyCount {
		return nil, fmt.Errorf("%w: aggregate dependency semantic edge budget exceeded", control.ErrCapacityExceeded)
	}
	records, err := readValidatedVersionDependencies(validator.database, version)
	if err != nil {
		return nil, err
	}
	node := &dependencyValidationNode{version: version, records: records}
	validator.nodes[key] = node
	validator.work.nodes++
	validator.work.edges += int64(len(records))
	if err := validator.validateAggregateWork(); err != nil {
		return nil, err
	}
	return node, nil
}

func (validator *dependencySemanticValidator) validateGraphSemantics(
	key dependencyVersionKey,
) error {
	if validator.semanticsValidated[key] {
		return nil
	}
	node, err := validator.loadStructuralNode(key)
	if err != nil {
		return err
	}
	semantics, err := validator.loadNodeSemantics(node)
	if err != nil {
		return err
	}
	for _, record := range node.records {
		targetKey := dependencyVersionKey{
			objectID: record.TargetObjectID,
			version:  record.TargetObjectVersion,
		}
		target, targetErr := validator.loadStructuralNode(targetKey)
		if targetErr != nil {
			return targetErr
		}
		// This comparison uses only sealed scalar authorities and therefore
		// occurs before opening the target definition blob.
		if !dependencyScopeAllows(node.version, target.version) {
			return fmt.Errorf("%w: dependency target is absent or forbidden", ErrCorrupt)
		}
		targetSemantics, targetErr := validator.loadNodeSemantics(target)
		if targetErr != nil {
			return targetErr
		}
		if targetSemantics.stage >= semantics.stage {
			return fmt.Errorf("%w: dependency points to a same or later stage", ErrCorrupt)
		}
		if !dependencyFieldsIntersect(semantics.inputFields, targetSemantics.outputFields) {
			return fmt.Errorf("%w: dependency field identity disagrees with definitions", ErrCorrupt)
		}
		if targetErr := validator.validateGraphSemantics(targetKey); targetErr != nil {
			return targetErr
		}
	}
	validator.semanticsValidated[key] = true
	return nil
}

func (validator *dependencySemanticValidator) loadNodeSemantics(
	node *dependencyValidationNode,
) (dependencyDefinitionSemantics, error) {
	if node.semanticsLoaded {
		return node.semantics, node.semanticsLoadErr
	}
	node.semanticsLoaded = true
	if node.version.State != StateActive {
		node.semanticsLoadErr = fmt.Errorf(
			"%w: active dependency source targets an inactive definition",
			ErrCorrupt,
		)
		return dependencyDefinitionSemantics{}, node.semanticsLoadErr
	}
	validator.work.queries += 2
	if err := validator.validateAggregateWork(); err != nil {
		node.semanticsLoadErr = err
		return dependencyDefinitionSemantics{}, err
	}
	decoded, definitionBytes, err := decodeVersionDefinition(validator.database, node.version)
	if err != nil {
		node.semanticsLoadErr = err
		return dependencyDefinitionSemantics{}, err
	}
	validator.work.definitionBytes += definitionBytes
	if err := validator.validateAggregateWork(); err != nil {
		node.semanticsLoadErr = err
		return dependencyDefinitionSemantics{}, err
	}
	if !decoded.ObjectTypeKnown {
		node.semanticsLoadErr = fmt.Errorf(
			"%w: recognized dependency source targets an opaque definition",
			ErrCorrupt,
		)
		return dependencyDefinitionSemantics{}, node.semanticsLoadErr
	}
	if err := validateDecodedIdentity(decoded, node.version); err != nil {
		node.semanticsLoadErr = err
		return dependencyDefinitionSemantics{}, err
	}
	node.semantics, err = dependencySemanticsFromDefinition(decoded.Definition)
	if err != nil {
		node.semanticsLoadErr = fmt.Errorf(
			"%w: decoded dependency semantics are invalid: %v",
			ErrCorrupt,
			err,
		)
		return dependencyDefinitionSemantics{}, node.semanticsLoadErr
	}
	return node.semantics, nil
}

func dependencyScopeAllows(source, target versionRecord) bool {
	if source.TenantID != target.TenantID {
		return false
	}
	switch source.SharingScope {
	case SharingScopePrivate:
		return target.SharingScope == SharingScopeGlobal ||
			target.SharingScope == SharingScopeApp && target.AppID == source.AppID ||
			target.SharingScope == SharingScopePrivate && target.AppID == source.AppID &&
				target.OwnerID == source.OwnerID
	case SharingScopeApp:
		return target.SharingScope == SharingScopeGlobal ||
			target.SharingScope == SharingScopeApp && target.AppID == source.AppID
	case SharingScopeGlobal:
		return target.SharingScope == SharingScopeGlobal
	default:
		return false
	}
}

func dependencySemanticsFromDefinition(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (dependencyDefinitionSemantics, error) {
	if definition == nil {
		return dependencyDefinitionSemantics{}, fmt.Errorf("definition is nil")
	}
	switch body := definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		if body == nil || body.FieldExtraction == nil || body.FieldExtraction.GetInputField() != "_raw" {
			return dependencyDefinitionSemantics{}, fmt.Errorf("field extraction input is not _raw")
		}
		outputs := make([]string, 0, 1)
		switch extraction := body.FieldExtraction.GetExtraction().(type) {
		case *opensplunkv1.FieldExtractionDefinition_Regex:
			if extraction == nil || extraction.Regex == nil {
				return dependencyDefinitionSemantics{}, fmt.Errorf("regex extraction is nil")
			}
			outputs = append(outputs, extraction.Regex.GetOutputFields()...)
		case *opensplunkv1.FieldExtractionDefinition_Json:
			if extraction == nil || extraction.Json == nil {
				return dependencyDefinitionSemantics{}, fmt.Errorf("JSON extraction is nil")
			}
			outputs = append(outputs, extraction.Json.GetOutputField())
		default:
			return dependencyDefinitionSemantics{}, fmt.Errorf("extraction kind is unsupported")
		}
		outputs = normalizedDependencyFields(outputs)
		if len(outputs) == 0 {
			return dependencyDefinitionSemantics{}, fmt.Errorf("field extraction has no output")
		}
		return dependencyDefinitionSemantics{
			stage: dependencyStageExtraction, outputFields: outputs,
		}, nil
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		if body == nil || body.FieldAlias == nil {
			return dependencyDefinitionSemantics{}, fmt.Errorf("field alias is nil")
		}
		return dependencyDefinitionSemantics{
			stage:        dependencyStageAlias,
			inputFields:  normalizedDependencyFields([]string{body.FieldAlias.GetSourceField()}),
			outputFields: normalizedDependencyFields([]string{body.FieldAlias.GetDestinationField()}),
		}, nil
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		if body == nil || body.CalculatedField == nil {
			return dependencyDefinitionSemantics{}, fmt.Errorf("calculated field is nil")
		}
		inputs, err := calculatedDependencyInputFields(body.CalculatedField.GetExpression())
		if err != nil {
			return dependencyDefinitionSemantics{}, err
		}
		return dependencyDefinitionSemantics{
			stage:        dependencyStageCalculated,
			inputFields:  inputs,
			outputFields: normalizedDependencyFields([]string{body.CalculatedField.GetDestinationField()}),
		}, nil
	default:
		return dependencyDefinitionSemantics{}, fmt.Errorf("definition body is unsupported")
	}
}

func normalizedDependencyFields(fields []string) []string {
	normalized := append([]string(nil), fields...)
	sort.Strings(normalized)
	normalized = compactNonemptyStrings(normalized)
	return normalized[:len(normalized):len(normalized)]
}

func compactNonemptyStrings(values []string) []string {
	write := 0
	for _, value := range values {
		if value == "" || write > 0 && values[write-1] == value {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}

func dependencyFieldsIntersect(left, right []string) bool {
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

func calculatedDependencyInputFields(expression string) ([]string, error) {
	parsed, err := spl.ParseScalarExpression(expression)
	if err != nil {
		return nil, fmt.Errorf("calculated expression is invalid")
	}
	collector := dependencyExpressionFieldCollector{fields: make(map[string]struct{})}
	if err := collector.visitScalar(parsed, 1); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(collector.fields))
	for field := range collector.fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields[:len(fields):len(fields)], nil
}

type dependencyExpressionFieldCollector struct {
	fields map[string]struct{}
	nodes  int
}

func (collector *dependencyExpressionFieldCollector) enter(depth int) error {
	if depth > maximumDependencyExpressionWalkDepth {
		return fmt.Errorf("calculated expression exceeds dependency walk depth")
	}
	collector.nodes++
	if collector.nodes > maximumDependencyExpressionWalkNodes {
		return fmt.Errorf("calculated expression exceeds dependency walk nodes")
	}
	return nil
}

func (collector *dependencyExpressionFieldCollector) visitScalar(expression spl.ScalarExpr, depth int) error {
	if err := collector.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		if expression == nil || expression.Field == "" {
			return fmt.Errorf("calculated expression contains an invalid field")
		}
		collector.fields[expression.Field] = struct{}{}
	case *spl.ScalarLiteralExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil literal")
		}
	case *spl.ScalarCallExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil call")
		}
		for _, argument := range expression.Arguments {
			if err := collector.visitScalar(argument, depth+1); err != nil {
				return err
			}
		}
	case *spl.ScalarIfExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil if")
		}
		if err := collector.visitWhere(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := collector.visitScalar(expression.True, depth+1); err != nil {
			return err
		}
		return collector.visitScalar(expression.False, depth+1)
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil case")
		}
		for _, branch := range expression.Branches {
			if err := collector.visitWhere(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := collector.visitScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("calculated expression contains an unsupported scalar node")
	}
	return nil
}

func (collector *dependencyExpressionFieldCollector) visitWhere(expression spl.WhereExpr, depth int) error {
	if err := collector.enter(depth); err != nil {
		return err
	}
	switch expression := expression.(type) {
	case *spl.WhereBoolExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil Boolean node")
		}
		if err := collector.visitWhere(expression.Left, depth+1); err != nil {
			return err
		}
		return collector.visitWhere(expression.Right, depth+1)
	case *spl.WhereNotExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil NOT node")
		}
		return collector.visitWhere(expression.Operand, depth+1)
	case *spl.WhereComparisonExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil comparison")
		}
		if err := collector.visitScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return collector.visitScalar(expression.Right, depth+1)
	case *spl.WhereScalarPredicateExpr:
		if expression == nil {
			return fmt.Errorf("calculated expression contains a nil predicate")
		}
		return collector.visitScalar(expression.Value, depth+1)
	default:
		return fmt.Errorf("calculated expression contains an unsupported Boolean node")
	}
}
