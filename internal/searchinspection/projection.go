package searchinspection

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	maximumAuthoredPlanStages = uint32(256)
	// The authored inspection ceiling predates the knowledge prelude. A valid
	// query may now contain the complete 256-operator generated prefix in
	// addition to every formerly admitted authored stage.
	maximumPlanStages = maximumAuthoredPlanStages +
		uint32(knowledgeprogram.MaximumObjects)
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
	maximumProjectedKnowledgeObjects = uint32(knowledgeprogram.MaximumObjects)
	maximumProjectedKnowledgeOutputs = uint32(knowledgeprogram.MaximumGeneratedFields)
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

// RedactedObjectProvenance is the complete safe identity for one knowledge
// object in an inspection response before a current-policy provenance
// authorizer exists. Ordinal is response-local and carries no catalog ID,
// version, name, owner, app, digest, selector, or definition location.
type RedactedObjectProvenance struct {
	Ordinal    uint32
	ObjectType opensplunkv1.KnowledgeObjectType
	Stage      opensplunkv1.KnowledgeSearchStage
}

// OutputProvenance associates one generated output occurrence with its
// redacted object. The same Field may appear more than once when distinct
// selector-disjoint objects legitimately target one destination.
type OutputProvenance struct {
	Field         string
	ObjectOrdinal uint32
}

// PlanStage is one safe logical stage summary. InputFields contains only
// sorted logical field reads. OutputFields contains only names produced or
// selected by the stage; expressions and literal values are never projected.
type PlanStage struct {
	Index            uint32
	Operator         string
	InputFields      []string
	OutputFields     []string
	SourceRange      *SourceRange
	KnowledgeObjects []RedactedObjectProvenance
	OutputProvenance []OutputProvenance
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
	knowledgeObjects uint32
	knowledgeOutputs uint32
}

type operatorDescription struct {
	name             string
	outputs          []string
	sourceRange      *spl.Range
	knowledgeObjects []RedactedObjectProvenance
	outputProvenance []OutputProvenance
}

type knowledgeOperatorShape uint8

const (
	knowledgeOperatorShapeOneToMany knowledgeOperatorShape = iota + 1
	knowledgeOperatorShapeOneToOne
	knowledgeOperatorShapeFusedOneToOne
)

type knowledgeOperatorRank uint8

const (
	knowledgeOperatorRankExtraction knowledgeOperatorRank = iota + 1
	knowledgeOperatorRankAlias
	knowledgeOperatorRankCalculated
)

type knowledgeOperatorContract struct {
	objectType opensplunkv1.KnowledgeObjectType
	stage      opensplunkv1.KnowledgeSearchStage
	shape      knowledgeOperatorShape
	rank       knowledgeOperatorRank
	repeatable bool
}

func inspectionKnowledgeOperator(
	value string,
) (knowledgeOperatorContract, bool) {
	switch value {
	case "ConditionalExtract":
		return knowledgeOperatorContract{
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			stage:      opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			shape:      knowledgeOperatorShapeOneToMany,
			rank:       knowledgeOperatorRankExtraction,
			repeatable: true,
		}, true
	case "ConditionalExtractJSON":
		return knowledgeOperatorContract{
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			stage:      opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			shape:      knowledgeOperatorShapeOneToOne,
			rank:       knowledgeOperatorRankExtraction,
			repeatable: true,
		}, true
	case "CopyFieldAlias":
		return knowledgeOperatorContract{
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			stage:      opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
			shape:      knowledgeOperatorShapeFusedOneToOne,
			rank:       knowledgeOperatorRankAlias,
		}, true
	case "ParallelExtend":
		return knowledgeOperatorContract{
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
			stage:      opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
			shape:      knowledgeOperatorShapeFusedOneToOne,
			rank:       knowledgeOperatorRankCalculated,
		}, true
	default:
		return knowledgeOperatorContract{}, false
	}
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
	authoredStages := 0
	for _, operator := range logical.Operators {
		if operator == nil {
			return LogicalPlan{}, invalidProjection("logical operator is nil")
		}
		if _, knowledge := inspectionKnowledgeOperator(operator.LogicalName()); !knowledge {
			authoredStages++
			if authoredStages > int(maximumAuthoredPlanStages) {
				return LogicalPlan{}, invalidProjection(
					"logical plan has too many authored stages",
				)
			}
		}
	}
	if len(source) > maximumProjectionSourceBytes ||
		!utf8.ValidString(source) {
		return LogicalPlan{}, invalidProjection("SPL source is not valid UTF-8")
	}

	analysis, err := plan.AnalyzeStages(logical)
	if err != nil {
		return LogicalPlan{}, invalidProjection("logical plan analysis failed")
	}
	if len(analysis.Stages) != len(logical.Operators) {
		return LogicalPlan{}, invalidProjection("logical stage analysis is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return LogicalPlan{}, err
	}

	budget := projectionBudget{}
	referencedFields, err := budget.projectFields(
		analysis.ReferencedFields,
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
		description, err := describeOperator(operator)
		if err != nil {
			return LogicalPlan{}, err
		}
		if err := budget.addString(description.name, maximumOperatorNameBytes); err != nil {
			return LogicalPlan{}, err
		}
		var projectedRange *SourceRange
		if description.sourceRange != nil {
			value, err := projectSourceRange(source, *description.sourceRange)
			if err != nil {
				return LogicalPlan{}, err
			}
			projectedRange = &value
		}

		inputFields, err := budget.projectFields(
			analysis.Stages[index].ReferencedFields,
			maximumStageFields,
		)
		if err != nil {
			return LogicalPlan{}, err
		}
		slices.Sort(description.outputs)
		description.outputs = slices.Compact(description.outputs)
		outputFields, err := budget.projectFields(
			description.outputs,
			maximumStageFields,
		)
		if err != nil {
			return LogicalPlan{}, err
		}
		knowledgeObjects, outputProvenance, err :=
			budget.projectKnowledgeProvenance(
				description.name,
				description.knowledgeObjects,
				description.outputProvenance,
				outputFields,
			)
		if err != nil {
			return LogicalPlan{}, err
		}
		stages[index] = PlanStage{
			Index:            uint32(index),
			Operator:         description.name,
			InputFields:      inputFields,
			OutputFields:     outputFields,
			SourceRange:      projectedRange,
			KnowledgeObjects: knowledgeObjects,
			OutputProvenance: outputProvenance,
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

func describeOperator(operator plan.Operator) (operatorDescription, error) {
	if operator == nil {
		return operatorDescription{}, invalidProjection("logical operator is nil")
	}

	description := operatorDescription{name: operator.LogicalName()}
	switch concrete := operator.(type) {
	case *plan.ConditionalExtract:
		if concrete == nil {
			return operatorDescription{}, invalidProjection("logical operator is nil")
		}
		extraction := concrete.Extraction()
		captures := extraction.Captures()
		if len(captures) == 0 || len(captures) > int(maximumStageFields) {
			return operatorDescription{}, invalidProjection(
				"logical ConditionalExtract has an invalid output inventory",
			)
		}
		description.outputs = make([]string, len(captures))
		for index, capture := range captures {
			description.outputs[index] = capture.Name()
		}
		if err := appendKnowledgeOrigin(
			&description,
			extraction.Origin(),
			description.outputs,
		); err != nil {
			return operatorDescription{}, err
		}
		return description, nil
	case *plan.ConditionalExtractJSON:
		if concrete == nil {
			return operatorDescription{}, invalidProjection("logical operator is nil")
		}
		extraction := concrete.Extraction()
		description.outputs = []string{extraction.Output()}
		if err := appendKnowledgeOrigin(
			&description,
			extraction.Origin(),
			description.outputs,
		); err != nil {
			return operatorDescription{}, err
		}
		return description, nil
	case *plan.CopyFieldAlias:
		if concrete == nil {
			return operatorDescription{}, invalidProjection("logical operator is nil")
		}
		assignments := concrete.Assignments()
		if len(assignments) == 0 || len(assignments) > int(maximumStageFields) {
			return operatorDescription{}, invalidProjection(
				"logical CopyFieldAlias has an invalid output inventory",
			)
		}
		description.outputs = make([]string, len(assignments))
		for index, assignment := range assignments {
			description.outputs[index] = assignment.Destination()
			if err := appendKnowledgeOrigin(
				&description,
				assignment.Origin(),
				[]string{description.outputs[index]},
			); err != nil {
				return operatorDescription{}, err
			}
		}
		return description, nil
	case *plan.ParallelExtend:
		if concrete == nil {
			return operatorDescription{}, invalidProjection("logical operator is nil")
		}
		assignments := concrete.Assignments()
		if len(assignments) == 0 || len(assignments) > int(maximumStageFields) {
			return operatorDescription{}, invalidProjection(
				"logical ParallelExtend has an invalid output inventory",
			)
		}
		description.outputs = make([]string, len(assignments))
		for index, assignment := range assignments {
			description.outputs[index] = assignment.Destination()
			if err := appendKnowledgeOrigin(
				&description,
				assignment.Origin(),
				[]string{description.outputs[index]},
			); err != nil {
				return operatorDescription{}, err
			}
		}
		return description, nil
	default:
		name, outputs, sourceRange, err := describeAuthoredOperator(operator)
		if err != nil {
			return operatorDescription{}, err
		}
		description.name = name
		description.outputs = outputs
		description.sourceRange = &sourceRange
		return description, nil
	}
}

func appendKnowledgeOrigin(
	description *operatorDescription,
	origin knowledgeprogram.Origin,
	outputs []string,
) error {
	if description == nil || len(outputs) == 0 {
		return invalidProjection("logical knowledge provenance is invalid")
	}
	contract, ok := inspectionKnowledgeOperator(description.name)
	if !ok ||
		origin.ResolutionOrdinal() >= maximumProjectedKnowledgeObjects ||
		origin.ObjectType() != contract.objectType || origin.Stage() != contract.stage {
		return invalidProjection("logical knowledge provenance is invalid")
	}
	object := RedactedObjectProvenance{
		Ordinal:    origin.ResolutionOrdinal(),
		ObjectType: origin.ObjectType(),
		Stage:      origin.Stage(),
	}
	description.knowledgeObjects = append(description.knowledgeObjects, object)
	for _, output := range outputs {
		description.outputProvenance = append(
			description.outputProvenance,
			OutputProvenance{
				Field:         output,
				ObjectOrdinal: object.Ordinal,
			},
		)
	}
	return nil
}

func describeAuthoredOperator(
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
	case *plan.StreamAggregate:
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

func (budget *projectionBudget) projectKnowledgeProvenance(
	operator string,
	objects []RedactedObjectProvenance,
	outputs []OutputProvenance,
	outputFields []string,
) ([]RedactedObjectProvenance, []OutputProvenance, error) {
	if budget == nil {
		return nil, nil, invalidProjection("projection budget is nil")
	}
	if len(objects) == 0 && len(outputs) == 0 {
		return nil, nil, nil
	}
	projectedObjects := slices.Clone(objects)
	slices.SortFunc(projectedObjects, func(left, right RedactedObjectProvenance) int {
		return int(left.Ordinal) - int(right.Ordinal)
	})
	projectedOutputs := slices.Clone(outputs)
	slices.SortFunc(projectedOutputs, compareOutputProvenance)
	contract, ok := inspectionKnowledgeOperator(operator)
	if !ok || !validateCanonicalKnowledgeProvenance(
		budget,
		projectedObjects,
		projectedOutputs,
		outputFields,
		contract,
	) {
		return nil, nil, invalidProjection(
			"logical knowledge provenance is invalid",
		)
	}
	return projectedObjects, projectedOutputs, nil
}

func validateCanonicalKnowledgeProvenance(
	budget *projectionBudget,
	objects []RedactedObjectProvenance,
	outputs []OutputProvenance,
	outputFields []string,
	contract knowledgeOperatorContract,
) bool {
	if budget == nil || len(objects) == 0 || len(outputs) == 0 ||
		budget.knowledgeObjects > maximumProjectedKnowledgeObjects ||
		budget.knowledgeOutputs > maximumProjectedKnowledgeOutputs ||
		uint64(len(objects)) > uint64(maximumProjectedKnowledgeObjects-budget.knowledgeObjects) ||
		uint64(len(outputs)) > uint64(maximumProjectedKnowledgeOutputs-budget.knowledgeOutputs) {
		return false
	}
	for index, object := range objects {
		if object.Ordinal >= maximumProjectedKnowledgeObjects ||
			object.ObjectType != contract.objectType || object.Stage != contract.stage ||
			(index > 0 && objects[index-1].Ordinal >= object.Ordinal) {
			return false
		}
	}

	var usedObjects []bool
	if len(objects) > 1 {
		usedObjects = make([]bool, len(objects))
	}
	fieldIndex := 0
	for index, output := range outputs {
		if budget.addString(
			output.Field,
			eventfields.MaximumNormalizedFieldNameBytes,
		) != nil ||
			(index > 0 && compareOutputProvenance(outputs[index-1], output) >= 0) {
			return false
		}
		objectIndex, found := slices.BinarySearchFunc(
			objects,
			output.ObjectOrdinal,
			func(object RedactedObjectProvenance, ordinal uint32) int {
				if object.Ordinal < ordinal {
					return -1
				}
				if object.Ordinal > ordinal {
					return 1
				}
				return 0
			},
		)
		if !found {
			return false
		}
		if usedObjects != nil {
			usedObjects[objectIndex] = true
		}
		if index == 0 || outputs[index-1].Field != output.Field {
			if fieldIndex >= len(outputFields) ||
				outputFields[fieldIndex] != output.Field {
				return false
			}
			fieldIndex++
		}
	}
	if fieldIndex != len(outputFields) {
		return false
	}
	for _, used := range usedObjects {
		if !used {
			return false
		}
	}

	budget.knowledgeObjects += uint32(len(objects))
	budget.knowledgeOutputs += uint32(len(outputs))
	return true
}

func compareOutputProvenance(left, right OutputProvenance) int {
	if comparison := strings.Compare(left.Field, right.Field); comparison != 0 {
		return comparison
	}
	if left.ObjectOrdinal < right.ObjectOrdinal {
		return -1
	}
	if left.ObjectOrdinal > right.ObjectOrdinal {
		return 1
	}
	return 0
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
