package plan

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// queryKnowledgePrelude distinguishes a deliberately admitted empty knowledge
// program from a legacy query that never crossed the knowledge boundary. The
// program is opaque and immutable outside knowledgeprogram; every opening
// returns an independent clone.
type queryKnowledgePrelude struct {
	program knowledgeprogram.Program
	set     bool
}

// KnowledgePrelude opens a detached copy of the exact admitted program. The
// boolean is false for legacy queries and malformed zero-value markers.
func (query *Query) KnowledgePrelude() (knowledgeprogram.Program, bool) {
	if query == nil || !query.knowledgePrelude.set || query.knowledgePrelude.program.IsZero() {
		return knowledgeprogram.Program{}, false
	}
	return query.knowledgePrelude.program.Clone(), true
}

// ConditionalExtract is one selector-guarded regex knowledge extraction. Its
// payload is private so only a validated knowledge program can mint executable
// provenance and selector authority.
type ConditionalExtract struct {
	extraction knowledgeprogram.RegexExtraction
}

func (*ConditionalExtract) operator()              {}
func (*ConditionalExtract) LogicalName() string    { return "ConditionalExtract" }
func (*ConditionalExtract) SourceRange() spl.Range { return spl.Range{} }
func (op *ConditionalExtract) Extraction() knowledgeprogram.RegexExtraction {
	if op == nil {
		return knowledgeprogram.RegexExtraction{}
	}
	return op.extraction
}

// ConditionalExtractJSON is one selector-guarded JSON knowledge extraction.
// Unlike authored spath, selecting an object or array yields no output.
type ConditionalExtractJSON struct {
	extraction knowledgeprogram.JSONExtraction
}

func (*ConditionalExtractJSON) operator()              {}
func (*ConditionalExtractJSON) LogicalName() string    { return "ConditionalExtractJSON" }
func (*ConditionalExtractJSON) SourceRange() spl.Range { return spl.Range{} }
func (op *ConditionalExtractJSON) Extraction() knowledgeprogram.JSONExtraction {
	if op == nil {
		return knowledgeprogram.JSONExtraction{}
	}
	return op.extraction
}

// CopyFieldAlias evaluates every alias assignment against one frozen
// extraction-stage input and publishes the fused stage atomically.
type CopyFieldAlias struct {
	assignments []knowledgeprogram.Alias
}

func (*CopyFieldAlias) operator()              {}
func (*CopyFieldAlias) LogicalName() string    { return "CopyFieldAlias" }
func (*CopyFieldAlias) SourceRange() spl.Range { return spl.Range{} }
func (op *CopyFieldAlias) Assignments() []knowledgeprogram.Alias {
	if op == nil {
		return nil
	}
	return slices.Clone(op.assignments)
}

// ParallelExtend evaluates every calculated assignment against one frozen
// alias-stage input and publishes the fused stage atomically.
type ParallelExtend struct {
	assignments []knowledgeprogram.Calculated
}

func (*ParallelExtend) operator()              {}
func (*ParallelExtend) LogicalName() string    { return "ParallelExtend" }
func (*ParallelExtend) SourceRange() spl.Range { return spl.Range{} }
func (op *ParallelExtend) Assignments() []knowledgeprogram.Calculated {
	if op == nil {
		return nil
	}
	return slices.Clone(op.assignments)
}

// ConvertKnowledgeCalculatedExpression independently reconstructs the plan
// expression sealed by one calculated-field operation. The retained semantic
// inventory must agree exactly with a fresh parse before any executable plan
// value is returned.
func ConvertKnowledgeCalculatedExpression(
	operation knowledgeprogram.Calculated,
) (ScalarExpression, error) {
	return convertKnowledgeCalculatedExpression(
		operation.Expression(),
		operation.InputFields(),
		operation.Nodes(),
		operation.Predicates(),
	)
}

func convertKnowledgeCalculatedExpression(
	canonical string,
	expectedInputFields []string,
	expectedNodes uint32,
	expectedPredicates uint32,
) (ScalarExpression, error) {
	parsed, err := spl.ParseScalarExpression(canonical)
	if err != nil {
		return nil, fmt.Errorf("convert knowledge calculated expression: parse: %w", err)
	}
	if spl.ScalarExpressionMayReturnBooleanFunction(parsed) {
		return nil, errors.New(
			"convert knowledge calculated expression: cannot directly assign a Boolean result",
		)
	}
	analysis, err := spl.AnalyzeScalarExpression(parsed)
	if err != nil {
		return nil, fmt.Errorf("convert knowledge calculated expression: analyze: %w", err)
	}
	if !slices.Equal(analysis.InputFields, expectedInputFields) {
		return nil, errors.New(
			"convert knowledge calculated expression: input-field inventory disagrees",
		)
	}
	if analysis.Nodes != expectedNodes {
		return nil, errors.New(
			"convert knowledge calculated expression: node inventory disagrees",
		)
	}
	if analysis.Predicates != expectedPredicates {
		return nil, errors.New(
			"convert knowledge calculated expression: predicate inventory disagrees",
		)
	}
	expression, err := convertScalarExpressionUnchecked(parsed)
	if err != nil {
		return nil, fmt.Errorf("convert knowledge calculated expression: convert: %w", err)
	}
	return expression, nil
}

// InjectKnowledgePrelude returns a detached query whose explicit executable
// knowledge prefix starts immediately after Scan. A valid empty program still
// seals the query marker, but adds no operators.
func InjectKnowledgePrelude(query *Query, program knowledgeprogram.Program) (*Query, error) {
	if query == nil {
		return nil, errors.New("inject knowledge prelude: query is nil")
	}
	if program.IsZero() {
		return nil, errors.New("inject knowledge prelude: program is invalid")
	}
	if len(query.Operators) == 0 {
		return nil, errors.New("inject knowledge prelude: query has no scan")
	}
	scan, ok := query.Operators[0].(*Scan)
	if !ok || scan == nil {
		return nil, errors.New("inject knowledge prelude: first operator is not a scan")
	}
	if query.knowledgePrelude.set || !query.knowledgePrelude.program.IsZero() {
		return nil, errors.New("inject knowledge prelude: query already has a knowledge program")
	}
	for _, operator := range query.Operators {
		if isKnowledgePreludeOperator(operator) {
			return nil, fmt.Errorf(
				"inject knowledge prelude: query already contains %s",
				operator.LogicalName(),
			)
		}
	}

	generated, err := knowledgePreludeOperators(program)
	if err != nil {
		return nil, fmt.Errorf("inject knowledge prelude: %w", err)
	}

	result := cloneQueryHeader(query)
	result.Operators = make([]Operator, 0, len(query.Operators)+len(generated))
	clonedScan := *scan
	clonedScan.Indexes = slices.Clone(scan.Indexes)
	result.Operators = append(result.Operators, &clonedScan)
	result.Operators = append(result.Operators, generated...)
	result.Operators = append(result.Operators, query.Operators[1:]...)
	result.knowledgePrelude = queryKnowledgePrelude{
		program: program.Clone(),
		set:     true,
	}
	return result, nil
}

// ValidateKnowledgePreludeIntegrity proves that the retained program and the
// explicit executable prefix still agree exactly. Legacy queries may omit
// both; a deliberately admitted empty program remains a present marker with
// no generated operators after Scan.
func ValidateKnowledgePreludeIntegrity(query *Query) error {
	if query == nil {
		return errors.New("validate knowledge prelude: query is nil")
	}
	markerPresent := query.knowledgePrelude.set
	programPresent := !query.knowledgePrelude.program.IsZero()
	if markerPresent != programPresent {
		return errors.New("validate knowledge prelude: marker is malformed")
	}
	if !markerPresent {
		for _, operator := range query.Operators {
			if isKnowledgePreludeOperator(operator) {
				return errors.New("validate knowledge prelude: legacy query contains a knowledge operator")
			}
		}
		return nil
	}
	if len(query.Operators) == 0 {
		return errors.New("validate knowledge prelude: query has no scan")
	}
	if scan, ok := query.Operators[0].(*Scan); !ok || scan == nil {
		return errors.New("validate knowledge prelude: first operator is not a scan")
	}

	expected, err := knowledgePreludeOperators(query.knowledgePrelude.program)
	if err != nil {
		return fmt.Errorf("validate knowledge prelude: %w", err)
	}
	if len(query.Operators) < 1+len(expected) {
		return errors.New("validate knowledge prelude: generated prefix is incomplete")
	}
	for index, want := range expected {
		if !knowledgeOperatorsEqual(query.Operators[index+1], want) {
			return fmt.Errorf("validate knowledge prelude: generated operator %d disagrees", index)
		}
	}
	for _, operator := range query.Operators[1+len(expected):] {
		if isKnowledgePreludeOperator(operator) {
			return errors.New("validate knowledge prelude: knowledge operator is outside the generated prefix")
		}
	}
	return nil
}

func knowledgePreludeOperators(program knowledgeprogram.Program) ([]Operator, error) {
	if program.IsZero() {
		return nil, errors.New("knowledge prelude program is invalid")
	}
	charges := program.Charges()
	generated := make([]Operator, 0, charges.GeneratedOperators)
	regex := program.RegexExtractions()
	json := program.JSONExtractions()
	aliases := program.Aliases()
	calculated := program.CalculatedFields()
	regexIndex, jsonIndex := 0, 0
	for _, kind := range program.OperatorKinds() {
		switch kind {
		case knowledgeprogram.OperatorConditionalExtract:
			if regexIndex >= len(regex) {
				return nil, errors.New("regex operator inventory disagrees")
			}
			generated = append(generated, &ConditionalExtract{extraction: regex[regexIndex]})
			regexIndex++
		case knowledgeprogram.OperatorConditionalExtractJSON:
			if jsonIndex >= len(json) {
				return nil, errors.New("JSON operator inventory disagrees")
			}
			generated = append(generated, &ConditionalExtractJSON{extraction: json[jsonIndex]})
			jsonIndex++
		case knowledgeprogram.OperatorCopyFieldAlias:
			if len(aliases) == 0 {
				return nil, errors.New("alias operator inventory disagrees")
			}
			generated = append(generated, &CopyFieldAlias{assignments: aliases})
		case knowledgeprogram.OperatorParallelExtend:
			if len(calculated) == 0 {
				return nil, errors.New("calculated operator inventory disagrees")
			}
			generated = append(generated, &ParallelExtend{assignments: calculated})
		default:
			return nil, errors.New("unknown operator kind")
		}
	}
	if uint32(len(generated)) != charges.GeneratedOperators || regexIndex != len(regex) || jsonIndex != len(json) {
		return nil, errors.New("operator inventory disagrees")
	}
	return generated, nil
}

func cloneQueryHeader(query *Query) *Query {
	result := *query
	result.Operators = slices.Clone(query.Operators)
	result.EffectiveIndexes = slices.Clone(query.EffectiveIndexes)
	result.OutputFields = slices.Clone(query.OutputFields)
	if query.DynamicOutput != nil {
		dynamic := *query.DynamicOutput
		dynamic.FixedFields = slices.Clone(query.DynamicOutput.FixedFields)
		result.DynamicOutput = &dynamic
	}
	return &result
}

func isKnowledgePreludeOperator(operator Operator) bool {
	switch operator.(type) {
	case *ConditionalExtract, *ConditionalExtractJSON, *CopyFieldAlias, *ParallelExtend:
		return true
	default:
		return false
	}
}

func knowledgeOperatorsEqual(got, want Operator) bool {
	switch want := want.(type) {
	case *ConditionalExtract:
		got, ok := got.(*ConditionalExtract)
		return ok && got != nil && want != nil &&
			regexExtractionsEqual(got.Extraction(), want.Extraction())
	case *ConditionalExtractJSON:
		got, ok := got.(*ConditionalExtractJSON)
		return ok && got != nil && want != nil &&
			jsonExtractionsEqual(got.Extraction(), want.Extraction())
	case *CopyFieldAlias:
		got, ok := got.(*CopyFieldAlias)
		return ok && got != nil && want != nil &&
			aliasesEqual(got.Assignments(), want.Assignments())
	case *ParallelExtend:
		got, ok := got.(*ParallelExtend)
		return ok && got != nil && want != nil &&
			calculatedEqual(got.Assignments(), want.Assignments())
	default:
		return false
	}
}

func regexExtractionsEqual(left, right knowledgeprogram.RegexExtraction) bool {
	if left.Origin() != right.Origin() || !selectorsEqual(left.Selector(), right.Selector()) ||
		left.Overwrite() != right.Overwrite() || left.Input() != right.Input() ||
		left.Pattern() != right.Pattern() || left.ProgramWorkUnits() != right.ProgramWorkUnits() {
		return false
	}
	leftCaptures, rightCaptures := left.Captures(), right.Captures()
	if len(leftCaptures) != len(rightCaptures) {
		return false
	}
	for index := range leftCaptures {
		if leftCaptures[index].Name() != rightCaptures[index].Name() ||
			leftCaptures[index].Group() != rightCaptures[index].Group() ||
			leftCaptures[index].DefinitionLocation() != rightCaptures[index].DefinitionLocation() {
			return false
		}
	}
	return true
}

func jsonExtractionsEqual(left, right knowledgeprogram.JSONExtraction) bool {
	return left.Origin() == right.Origin() && selectorsEqual(left.Selector(), right.Selector()) &&
		left.Overwrite() == right.Overwrite() && left.Input() == right.Input() &&
		left.Path() == right.Path() && slices.Equal(left.Steps(), right.Steps()) &&
		left.Output() == right.Output() &&
		left.OutputDefinitionLocation() == right.OutputDefinitionLocation() &&
		left.EvaluationWorkUnits() == right.EvaluationWorkUnits()
}

func aliasesEqual(left, right []knowledgeprogram.Alias) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Origin() != right[index].Origin() ||
			!selectorsEqual(left[index].Selector(), right[index].Selector()) ||
			left[index].Overwrite() != right[index].Overwrite() ||
			left[index].Source() != right[index].Source() ||
			left[index].Destination() != right[index].Destination() {
			return false
		}
	}
	return true
}

func calculatedEqual(left, right []knowledgeprogram.Calculated) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Origin() != right[index].Origin() ||
			!selectorsEqual(left[index].Selector(), right[index].Selector()) ||
			left[index].Overwrite() != right[index].Overwrite() ||
			left[index].Destination() != right[index].Destination() ||
			left[index].Expression() != right[index].Expression() ||
			!slices.Equal(left[index].InputFields(), right[index].InputFields()) ||
			left[index].Nodes() != right[index].Nodes() ||
			left[index].Predicates() != right[index].Predicates() {
			return false
		}
	}
	return true
}

func selectorsEqual(left, right knowledgeprogram.Selector) bool {
	if !bytes.Equal(left.CanonicalBytes(), right.CanonicalBytes()) {
		return false
	}
	for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
		leftProgram, leftConstrained := left.RuntimeProgram(dimension)
		rightProgram, rightConstrained := right.RuntimeProgram(dimension)
		if leftConstrained != rightConstrained {
			return false
		}
		if leftConstrained &&
			(!slices.Equal(leftProgram.ExactLiterals, rightProgram.ExactLiterals) ||
				leftProgram.WildcardRE2 != rightProgram.WildcardRE2 ||
				leftProgram.Assessment != rightProgram.Assessment) {
			return false
		}
	}
	return true
}

func (analyzer *queryAnalyzer) visitKnowledgeSelector(
	selector knowledgeprogram.Selector,
	depth int,
) error {
	if len(selector.CanonicalBytes()) == 0 {
		return errors.New("analyze logical query: knowledge selector is invalid")
	}
	for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
		if _, constrained := selector.RuntimeProgram(dimension); !constrained {
			continue
		}
		if err := analyzer.addKnowledgeField(dimension.String(), depth); err != nil {
			return err
		}
	}
	return nil
}

func (analyzer *queryAnalyzer) validateKnowledgeField(name string, depth int) error {
	if err := analyzer.enterKnowledge(depth); err != nil {
		return err
	}
	field, err := ResolveField(name, spl.Range{})
	if err != nil {
		return err
	}
	if field.Name == "" {
		return errors.New("analyze logical query: field name is empty")
	}
	return nil
}

func (analyzer *queryAnalyzer) addKnowledgeField(name string, depth int) error {
	if err := analyzer.validateKnowledgeField(name, depth); err != nil {
		return err
	}
	analyzer.fields[name] = struct{}{}
	return nil
}
