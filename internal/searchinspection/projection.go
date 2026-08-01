package searchinspection

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	maximumPlanStages  = uint32(256)
	maximumStageFields = uint32(1_024)
	// A static schema may accumulate fields across otherwise bounded stages.
	// Every planner-built final field is represented by one of plan.Analyze's
	// globally bounded structural nodes, so use that 4,096-node ceiling without
	// conflating final shape with one stage.
	maximumFinalOutputFields = uint32(4_096)
	// Projection accounts for analyzed fields again in four categories: the
	// full read set, per-stage reads, per-stage writes, and final shape.
	maximumProjectedFieldOccurrences = uint64(16_384)
	maximumProjectedStringBytes      = uint64(1 << 20)
	maximumOperatorNameBytes         = 32
	maximumDynamicFields             = uint16(1_024)
	maximumProjectionSourceBytes     = 16 << 10
)

// OutputKind describes whether the final logical relation has an open,
// statically known, or runtime-wide schema.
type OutputKind uint8

const (
	OutputKindInvalid OutputKind = iota
	OutputKindOpen
	OutputKindStatic
	OutputKindDynamic
)

// SourcePosition is one bounded UTF-8 SPL source coordinate. ByteOffset is
// zero-based; Line and Column are one-based Unicode-scalar coordinates.
type SourcePosition struct {
	ByteOffset uint64
	Line       uint32
	Column     uint32
}

// SourceRange is a half-open range into the retained SPL source.
type SourceRange struct {
	Start SourcePosition
	End   SourcePosition
}

// PlanStage is one safe logical stage summary. InputFields contains only
// sorted logical field reads. OutputFields contains only names produced or
// selected by the stage; expressions and literal values are never projected.
type PlanStage struct {
	Index        uint32
	Operator     string
	InputFields  []string
	OutputFields []string
	SourceRange  SourceRange
}

// OutputShape is the final relation's bounded public schema. Fields preserves
// final output order; for dynamic output it is the fixed prefix.
type OutputShape struct {
	Kind             OutputKind
	Fields           []string
	MaxDynamicFields uint16
}

// LogicalPlan is a detached, bounded projection safe to keep separate from
// the administrator-sensitive generated SQL and EXPLAIN text.
type LogicalPlan struct {
	Stages           []PlanStage
	ReferencedFields []string
	Output           OutputShape
}

type projectionBudget struct {
	fieldOccurrences uint64
	stringBytes      uint64
}

func projectLogicalPlan(
	ctx context.Context,
	logical *plan.Query,
	source string,
) (LogicalPlan, error) {
	if ctx == nil {
		return LogicalPlan{}, invalidProjection("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return LogicalPlan{}, err
	}
	if logical == nil || len(logical.Operators) == 0 {
		return LogicalPlan{}, invalidProjection("logical plan is empty")
	}
	if len(logical.Operators) > int(maximumPlanStages) {
		return LogicalPlan{}, invalidProjection("logical plan has too many stages")
	}
	if len(source) > maximumProjectionSourceBytes ||
		!utf8.ValidString(source) {
		return LogicalPlan{}, invalidProjection("SPL source is not valid UTF-8")
	}

	fullAnalysis, err := plan.Analyze(logical)
	if err != nil {
		return LogicalPlan{}, invalidProjection("logical plan analysis failed")
	}
	if err := ctx.Err(); err != nil {
		return LogicalPlan{}, err
	}

	budget := projectionBudget{}
	referencedFields, err := budget.projectFields(
		fullAnalysis.ReferencedFields,
		maximumStageFields,
	)
	if err != nil {
		return LogicalPlan{}, err
	}
	output, err := projectOutputShape(&budget, logical)
	if err != nil {
		return LogicalPlan{}, err
	}

	stages := make([]PlanStage, len(logical.Operators))
	for index, operator := range logical.Operators {
		if err := ctx.Err(); err != nil {
			return LogicalPlan{}, err
		}
		operatorName, stageOutputs, sourceRange, err :=
			describeOperator(operator)
		if err != nil {
			return LogicalPlan{}, err
		}
		if err := budget.addString(operatorName, maximumOperatorNameBytes); err != nil {
			return LogicalPlan{}, err
		}
		projectedRange, err := projectSourceRange(source, sourceRange)
		if err != nil {
			return LogicalPlan{}, err
		}

		stageAnalysis, err := plan.Analyze(&plan.Query{
			Operators: []plan.Operator{operator},
		})
		if err != nil {
			return LogicalPlan{}, invalidProjection("logical stage analysis failed")
		}
		inputFields, err := budget.projectFields(
			stageAnalysis.ReferencedFields,
			maximumStageFields,
		)
		if err != nil {
			return LogicalPlan{}, err
		}
		slices.Sort(stageOutputs)
		stageOutputs = slices.Compact(stageOutputs)
		outputFields, err := budget.projectFields(
			stageOutputs,
			maximumStageFields,
		)
		if err != nil {
			return LogicalPlan{}, err
		}
		stages[index] = PlanStage{
			Index:        uint32(index),
			Operator:     operatorName,
			InputFields:  inputFields,
			OutputFields: outputFields,
			SourceRange:  projectedRange,
		}
	}
	if err := ctx.Err(); err != nil {
		return LogicalPlan{}, err
	}
	return LogicalPlan{
		Stages:           stages,
		ReferencedFields: referencedFields,
		Output:           output,
	}, nil
}

func describeOperator(
	operator plan.Operator,
) (string, []string, spl.Range, error) {
	if operator == nil {
		return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
	}

	var outputs []string
	switch concrete := operator.(type) {
	case *plan.Scan:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
	case *plan.Filter:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
	case *plan.Project:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		if len(concrete.Fields) > int(maximumStageFields) {
			return "", nil, spl.Range{}, invalidProjection(
				"logical Project has too many fields",
			)
		}
		switch concrete.Mode {
		case plan.ProjectModeInclude, plan.ProjectModeTable:
			outputs = fieldRefNames(concrete.Fields)
		case plan.ProjectModeExclude:
		default:
			return "", nil, spl.Range{}, invalidProjection(
				"logical Project mode is invalid",
			)
		}
	case *plan.Extend:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		if len(concrete.Assignments) > int(maximumStageFields) {
			return "", nil, spl.Range{}, invalidProjection(
				"logical Extend has too many assignments",
			)
		}
		outputs = make([]string, len(concrete.Assignments))
		for index, assignment := range concrete.Assignments {
			outputs[index] = assignment.Output.Name
		}
	case *plan.TimeBucket:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Output.Name}
	case *plan.NumericBucket:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Output.Name}
	case *plan.Extract:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		if len(concrete.Captures) > int(maximumStageFields) {
			return "", nil, spl.Range{}, invalidProjection(
				"logical Extract has too many captures",
			)
		}
		outputs = make([]string, len(concrete.Captures))
		for index, capture := range concrete.Captures {
			outputs[index] = capture.Output.Name
		}
	case *plan.ExtractJSON:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Output.Name}
	case *plan.Rename:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		if len(concrete.Assignments) > int(maximumStageFields) {
			return "", nil, spl.Range{}, invalidProjection(
				"logical Rename has too many assignments",
			)
		}
		outputs = make([]string, len(concrete.Assignments))
		for index, assignment := range concrete.Assignments {
			outputs[index] = assignment.Destination.Name
		}
	case *plan.Aggregate:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		if len(concrete.GroupBy) > int(maximumStageFields) ||
			len(concrete.Measures) >
				int(maximumStageFields)-len(concrete.GroupBy) {
			return "", nil, spl.Range{}, invalidProjection(
				"logical Aggregate has too many outputs",
			)
		}
		outputs = fieldRefNames(concrete.GroupBy)
		for _, measure := range concrete.Measures {
			outputs = append(outputs, measure.Output)
		}
	case *plan.EventAggregate:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Measure.Output}
	case *plan.Timechart:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{"_time"}
		if concrete.Split == nil {
			outputs = append(outputs, concrete.Measure.Output)
		}
	case *plan.Chart:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Over.Name}
	case *plan.Window:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
		outputs = []string{concrete.Output}
	case *plan.Sort:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
	case *plan.Deduplicate:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
	case *plan.Limit:
		if concrete == nil {
			return "", nil, spl.Range{}, invalidProjection("logical operator is nil")
		}
	default:
		return "", nil, spl.Range{}, invalidProjection(
			"logical operator is unsupported",
		)
	}
	return operator.LogicalName(), outputs, operator.SourceRange(), nil
}

func projectOutputShape(
	budget *projectionBudget,
	logical *plan.Query,
) (OutputShape, error) {
	if logical.DynamicOutput != nil {
		if len(logical.OutputFields) != 0 ||
			len(logical.DynamicOutput.FixedFields) >
				int(maximumStageFields) ||
			logical.DynamicOutput.MaxSeries == 0 ||
			logical.DynamicOutput.MaxSeries > maximumDynamicFields ||
			len(logical.DynamicOutput.FixedFields) == 0 {
			return OutputShape{}, invalidProjection(
				"logical dynamic output is invalid",
			)
		}
		if hasDuplicateStrings(logical.DynamicOutput.FixedFields) {
			return OutputShape{}, invalidProjection(
				"logical dynamic output has duplicate fields",
			)
		}
		fields, err := budget.projectFields(
			logical.DynamicOutput.FixedFields,
			maximumStageFields,
		)
		if err != nil {
			return OutputShape{}, err
		}
		return OutputShape{
			Kind:             OutputKindDynamic,
			Fields:           fields,
			MaxDynamicFields: logical.DynamicOutput.MaxSeries,
		}, nil
	}
	if len(logical.OutputFields) == 0 {
		return OutputShape{Kind: OutputKindOpen}, nil
	}
	if len(logical.OutputFields) > int(maximumFinalOutputFields) {
		return OutputShape{}, invalidProjection(
			"logical static output has too many fields",
		)
	}
	if hasDuplicateStrings(logical.OutputFields) {
		return OutputShape{}, invalidProjection(
			"logical static output has duplicate fields",
		)
	}
	fields, err := budget.projectFields(
		logical.OutputFields,
		maximumFinalOutputFields,
	)
	if err != nil {
		return OutputShape{}, err
	}
	return OutputShape{Kind: OutputKindStatic, Fields: fields}, nil
}

func (budget *projectionBudget) projectFields(
	fields []string,
	maximum uint32,
) ([]string, error) {
	if budget == nil {
		return nil, invalidProjection("projection budget is nil")
	}
	if len(fields) > int(maximum) {
		return nil, invalidProjection("logical field count exceeds its bound")
	}
	if budget.fieldOccurrences+uint64(len(fields)) >
		maximumProjectedFieldOccurrences {
		return nil, invalidProjection(
			"logical field occurrences exceed their bound",
		)
	}
	projected := make([]string, len(fields))
	for index, field := range fields {
		if err := budget.addString(
			field,
			eventfields.MaximumNormalizedFieldNameBytes,
		); err != nil {
			return nil, err
		}
		projected[index] = strings.Clone(field)
	}
	budget.fieldOccurrences += uint64(len(fields))
	return projected, nil
}

func (budget *projectionBudget) addString(value string, maximum int) error {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return invalidProjection("logical projection contains an invalid string")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalidProjection(
				"logical projection contains a control character",
			)
		}
	}
	valueBytes := uint64(len(value))
	if valueBytes > maximumProjectedStringBytes-budget.stringBytes {
		return invalidProjection("logical projection exceeds its byte bound")
	}
	budget.stringBytes += valueBytes
	return nil
}

func projectSourceRange(source string, value spl.Range) (SourceRange, error) {
	start, ok := exactSourcePosition(source, value.Start)
	if !ok {
		return SourceRange{}, invalidProjection(
			"logical stage start position is invalid",
		)
	}
	end, ok := exactSourcePosition(source, value.End)
	if !ok ||
		value.End.Offset < value.Start.Offset ||
		value.End.Line < value.Start.Line ||
		(value.End.Line == value.Start.Line &&
			value.End.Column < value.Start.Column) {
		return SourceRange{}, invalidProjection(
			"logical stage end position is invalid",
		)
	}
	return SourceRange{Start: start, End: end}, nil
}

func exactSourcePosition(
	source string,
	value spl.Position,
) (SourcePosition, bool) {
	if value.Offset < 0 || value.Offset > len(source) ||
		value.Line <= 0 || value.Column <= 0 ||
		uint64(value.Line) > math.MaxUint32 ||
		uint64(value.Column) > math.MaxUint32 {
		return SourcePosition{}, false
	}
	offset, line, column := 0, 1, 1
	for offset < value.Offset {
		character, width := utf8.DecodeRuneInString(source[offset:])
		if character == utf8.RuneError && width == 1 {
			return SourcePosition{}, false
		}
		if offset+width > value.Offset {
			return SourcePosition{}, false
		}
		offset += width
		if character == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	if line != value.Line || column != value.Column {
		return SourcePosition{}, false
	}
	return SourcePosition{
		ByteOffset: uint64(value.Offset),
		Line:       uint32(value.Line),
		Column:     uint32(value.Column),
	}, true
}

func fieldRefNames(fields []plan.FieldRef) []string {
	names := make([]string, len(fields))
	for index, field := range fields {
		names[index] = field.Name
	}
	return names
}

func hasDuplicateStrings(values []string) bool {
	if len(values) < 2 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func invalidProjection(message string) error {
	if message == "" {
		return searchjobs.ErrInvalidResult
	}
	return fmt.Errorf("%w: safe logical plan %s", searchjobs.ErrInvalidResult, message)
}
