package searchinspection

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	maximumInspectionExplainTextBytes         = 1 << 20
	maximumInspectionExplainLines             = 4_096
	maximumInspectionExplainLineBytes         = 16 << 10
	maximumInspectionDiagnosticQueryIDBytes   = 128
	maximumInspectionPhysicalNodes            = 4_096
	maximumInspectionPhysicalReads            = 256
	maximumInspectionPhysicalHeaders          = 4_096
	maximumInspectionPhysicalIndexes          = 4_096
	maximumInspectionPhysicalIndexKeys        = 64
	maximumInspectionPhysicalMetadataBytes    = 16 << 10
	maximumInspectionPhysicalTotalStringBytes = 1 << 20
)

// ValidateResult verifies a complete inspection result received through the
// service interface before an administrator-facing transport publishes it.
// It accepts only the canonical bounded projections produced by Service:
// EXPLAIN is parsed again and its safe physical projection must exactly match
// PhysicalPlan. Errors contain fixed diagnostics and never echo SQL, plan
// text, query IDs, field names, or other administrator-sensitive content.
func ValidateResult(result Result) error {
	if result.KnowledgeSnapshot != nil {
		if err := knowledgesnapshot.ValidateSummary(
			result.KnowledgeSnapshot,
		); err != nil {
			return invalidInspectionResult(
				"has an invalid knowledge snapshot summary",
			)
		}
	}
	if !validInspectionSQL(result.GeneratedSQL) {
		return invalidInspectionResult("has invalid generated SQL")
	}
	knowledgeObjects, ok := validInspectionLogicalPlan(result.Plan)
	if !ok || !validInspectionKnowledgeBinding(
		knowledgeObjects,
		result.KnowledgeSnapshot,
	) {
		return invalidInspectionResult("has an invalid logical plan")
	}
	if !validInspectionPhysicalBounds(result.PhysicalPlan) {
		return invalidInspectionResult("has an invalid physical plan")
	}
	if !validInspectionExplainBounds(
		result.ExplainText,
		result.DiagnosticQueryID,
	) {
		return invalidInspectionResult("has invalid EXPLAIN diagnostics")
	}

	reparsed, err := queryexec.ParseExplainPlan(queryexec.ExplainResult{
		Text:    result.ExplainText,
		QueryID: result.DiagnosticQueryID,
	})
	if err != nil {
		return invalidInspectionResult("has invalid EXPLAIN diagnostics")
	}
	if !reflect.DeepEqual(reparsed, result.PhysicalPlan) {
		return invalidInspectionResult(
			"physical plan does not match EXPLAIN diagnostics",
		)
	}
	return nil
}

func validInspectionSQL(value string) bool {
	if value == "" ||
		len(value) > maximumGeneratedSQLBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) == "" {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) &&
			character != '\t' &&
			character != '\n' &&
			character != '\r' {
			return false
		}
	}
	return true
}

func validInspectionExplainBounds(text, queryID string) bool {
	if text == "" ||
		len(text) > maximumInspectionExplainTextBytes ||
		queryID == "" ||
		len(queryID) > maximumInspectionDiagnosticQueryIDBytes {
		return false
	}

	lineStart, lineCount := 0, 1
	for index := 0; index < len(text); index++ {
		if text[index] != '\n' {
			continue
		}
		if index == lineStart ||
			index-lineStart > maximumInspectionExplainLineBytes ||
			lineCount >= maximumInspectionExplainLines {
			return false
		}
		lineStart = index + 1
		lineCount++
	}
	return lineStart < len(text) &&
		len(text)-lineStart <= maximumInspectionExplainLineBytes
}

func validInspectionLogicalPlan(
	logical LogicalPlan,
) ([]RedactedObjectProvenance, bool) {
	if len(logical.Stages) == 0 ||
		len(logical.Stages) > int(maximumPlanStages) {
		return nil, false
	}

	budget := projectionBudget{}
	knowledgeObjects := make([]RedactedObjectProvenance, 0)
	knowledgePrefixEnded := false
	var knowledgeRank knowledgeOperatorRank
	authoredStages := 0
	for index, stage := range logical.Stages {
		if (index == 0) != (stage.Operator == "Scan") {
			return nil, false
		}
		contract, knowledge := inspectionKnowledgeOperator(stage.Operator)
		if knowledge {
			if index == 0 || knowledgePrefixEnded ||
				contract.rank < knowledgeRank ||
				(contract.rank == knowledgeRank && !contract.repeatable) ||
				stage.SourceRange != nil ||
				!validInspectionKnowledgeStage(
					&budget,
					stage,
					contract,
				) {
				return nil, false
			}
			knowledgeRank = contract.rank
			knowledgeObjects = append(knowledgeObjects, stage.KnowledgeObjects...)
		} else {
			authoredStages++
			if authoredStages > int(maximumAuthoredPlanStages) {
				return nil, false
			}
			if index > 0 {
				knowledgePrefixEnded = true
			}
			if stage.SourceRange == nil ||
				len(stage.KnowledgeObjects) != 0 ||
				len(stage.OutputProvenance) != 0 ||
				!validInspectionSourceRange(*stage.SourceRange) {
				return nil, false
			}
		}
		if stage.Index != uint32(index) ||
			budget.addString(
				stage.Operator,
				maximumOperatorNameBytes,
			) != nil ||
			!supportedInspectionOperator(stage.Operator) ||
			!validInspectionFields(
				&budget,
				stage.InputFields,
				maximumStageFields,
				true,
			) ||
			!validInspectionFields(
				&budget,
				stage.OutputFields,
				maximumStageFields,
				true,
			) {
			return nil, false
		}
	}
	for index, object := range knowledgeObjects {
		if object.Ordinal != uint32(index) {
			return nil, false
		}
	}
	if !validInspectionFields(
		&budget,
		logical.ReferencedFields,
		maximumStageFields,
		true,
	) {
		return nil, false
	}
	if !validInspectionOutputShape(&budget, logical.Output) {
		return nil, false
	}
	return knowledgeObjects, true
}

func supportedInspectionOperator(value string) bool {
	if _, ok := inspectionKnowledgeOperator(value); ok {
		return true
	}
	switch value {
	case "Scan",
		"Filter",
		"Project",
		"Extend",
		"TimeBucket",
		"NumericBucket",
		"Extract",
		"ExtractJSON",
		"Rename",
		"Aggregate",
		"EventAggregate",
		"StreamAggregate",
		"Timechart",
		"Chart",
		"Window",
		"Sort",
		"Deduplicate",
		"Limit":
		return true
	default:
		return false
	}
}

func validInspectionKnowledgeStage(
	budget *projectionBudget,
	stage PlanStage,
	contract knowledgeOperatorContract,
) bool {
	switch contract.shape {
	case knowledgeOperatorShapeOneToMany:
		if len(stage.KnowledgeObjects) != 1 {
			return false
		}
	case knowledgeOperatorShapeOneToOne:
		if len(stage.KnowledgeObjects) != 1 ||
			len(stage.OutputProvenance) != 1 ||
			len(stage.OutputFields) != 1 {
			return false
		}
	case knowledgeOperatorShapeFusedOneToOne:
		if len(stage.KnowledgeObjects) != len(stage.OutputProvenance) {
			return false
		}
	default:
		return false
	}

	return validateCanonicalKnowledgeProvenance(
		budget,
		stage.KnowledgeObjects,
		stage.OutputProvenance,
		stage.OutputFields,
		contract,
	)
}

func validInspectionKnowledgeBinding(
	objects []RedactedObjectProvenance,
	summary *opensplunkv1.KnowledgeSnapshotSummary,
) bool {
	if summary == nil {
		return len(objects) == 0
	}
	objectCount := summary.GetRef().GetObjectCount()
	if uint64(len(objects)) != uint64(objectCount) {
		return false
	}
	for _, object := range objects {
		if object.Ordinal >= objectCount {
			return false
		}
		if int(object.Ordinal) >= len(summary.GetObjects()) {
			continue
		}
		retained := summary.GetObjects()[object.Ordinal]
		if retained.GetResolutionOrdinal() != object.Ordinal ||
			retained.GetObjectType() != object.ObjectType ||
			retained.GetStage() != object.Stage {
			return false
		}
	}
	return true
}

func validInspectionFields(
	budget *projectionBudget,
	fields []string,
	maximum uint32,
	strictlySorted bool,
) bool {
	if budget == nil ||
		len(fields) > int(maximum) ||
		uint64(len(fields)) >
			maximumProjectedFieldOccurrences-budget.fieldOccurrences {
		return false
	}
	var previous string
	for index, field := range fields {
		if budget.addString(
			field,
			eventfields.MaximumNormalizedFieldNameBytes,
		) != nil {
			return false
		}
		resolved, err := plan.ResolveField(field, spl.Range{})
		if err != nil ||
			resolved.Name != field ||
			(strictlySorted && index > 0 && previous >= field) {
			return false
		}
		previous = field
	}
	budget.fieldOccurrences += uint64(len(fields))
	return true
}

func validInspectionOutputShape(
	budget *projectionBudget,
	output OutputShape,
) bool {
	switch output.Kind {
	case OutputKindOpen:
		return len(output.Fields) == 0 &&
			output.MaxDynamicFields == 0
	case OutputKindStatic:
		return len(output.Fields) > 0 &&
			len(output.Fields) <= int(maximumFinalOutputFields) &&
			output.MaxDynamicFields == 0 &&
			validInspectionFields(
				budget,
				output.Fields,
				maximumFinalOutputFields,
				false,
			) &&
			!hasDuplicateStrings(output.Fields)
	case OutputKindDynamic:
		return len(output.Fields) > 0 &&
			len(output.Fields) <= int(maximumStageFields) &&
			output.MaxDynamicFields > 0 &&
			output.MaxDynamicFields <= maximumDynamicFields &&
			validInspectionFields(
				budget,
				output.Fields,
				maximumStageFields,
				false,
			) &&
			!hasDuplicateStrings(output.Fields)
	default:
		return false
	}
}

func validInspectionSourceRange(value SourceRange) bool {
	if !validInspectionSourcePosition(value.Start) ||
		!validInspectionSourcePosition(value.End) ||
		value.End.ByteOffset < value.Start.ByteOffset ||
		value.End.Line < value.Start.Line ||
		(value.End.Line == value.Start.Line &&
			value.End.Column < value.Start.Column) {
		return false
	}

	byteDistance := value.End.ByteOffset - value.Start.ByteOffset
	if byteDistance == 0 {
		return value.Start == value.End
	}
	lineDistance := uint64(value.End.Line - value.Start.Line)
	if lineDistance > byteDistance {
		return false
	}
	if lineDistance == 0 {
		columnDistance := uint64(value.End.Column - value.Start.Column)
		return columnDistance > 0 && columnDistance <= byteDistance
	}
	return uint64(value.End.Column) <= byteDistance-lineDistance+1
}

func validInspectionSourcePosition(value SourcePosition) bool {
	if value.ByteOffset > maximumProjectionSourceBytes ||
		value.Line == 0 ||
		value.Column == 0 {
		return false
	}
	maximumCoordinate := value.ByteOffset + 1
	return uint64(value.Line) <= maximumCoordinate &&
		uint64(value.Column) <= maximumCoordinate &&
		uint64(value.Line)+uint64(value.Column) <=
			value.ByteOffset+2
}

func validInspectionPhysicalBounds(physical queryexec.ExplainPlan) bool {
	if len(physical.NodeTypes) > maximumInspectionPhysicalNodes ||
		len(physical.Reads) > maximumInspectionPhysicalReads {
		return false
	}

	var stringBytes, headers, indexes uint64
	for _, nodeType := range physical.NodeTypes {
		if !addInspectionPhysicalString(
			&stringBytes,
			nodeType,
			false,
		) ||
			!supportedInspectionPhysicalNode(nodeType) {
			return false
		}
	}
	for _, read := range physical.Reads {
		if uint64(len(read.Columns)) >
			maximumInspectionPhysicalHeaders-headers ||
			uint64(len(read.Indexes)) >
				maximumInspectionPhysicalIndexes-indexes {
			return false
		}
		headers += uint64(len(read.Columns))
		indexes += uint64(len(read.Indexes))
		for _, column := range read.Columns {
			if !addInspectionPhysicalString(
				&stringBytes,
				column,
				false,
			) {
				return false
			}
		}
		for _, index := range read.Indexes {
			if !validInspectionPhysicalIndex(&stringBytes, index) {
				return false
			}
		}
	}
	return true
}

func validInspectionPhysicalIndex(
	stringBytes *uint64,
	index queryexec.ExplainIndex,
) bool {
	if len(index.Keys) > maximumInspectionPhysicalIndexKeys ||
		index.SelectedParts > index.InitialParts ||
		index.SelectedGranules > index.InitialGranules ||
		!addInspectionPhysicalString(stringBytes, index.Type, false) ||
		!supportedInspectionIndexType(index.Type) ||
		!addInspectionPhysicalString(stringBytes, index.Name, true) ||
		!validInspectionIndexName(index.Type, index.Name) {
		return false
	}
	for _, key := range index.Keys {
		if !addInspectionPhysicalString(stringBytes, key, false) {
			return false
		}
	}
	return true
}

func supportedInspectionPhysicalNode(value string) bool {
	switch value {
	case "Aggregating",
		"CreatingSet",
		"CreatingSets",
		"Expression",
		"Filter",
		"Join",
		"JoinLazyColumnsStep",
		"LazilyReadFromMergeTree",
		"Limit",
		"MaterializingCTE",
		"MaterializingCTEs",
		"ReadFromMemoryStorage",
		"ReadFromMergeTree",
		"ReadFromSystemNumbers",
		"ReadNothing",
		"Sorting",
		"Union",
		"Window":
		return true
	default:
		return false
	}
}

func supportedInspectionIndexType(value string) bool {
	switch value {
	case "MinMax", "Partition", "PrimaryKey", "Skip":
		return true
	default:
		return false
	}
}

func validInspectionIndexName(indexType, name string) bool {
	if indexType != "Skip" {
		return name == ""
	}
	switch name {
	case "",
		"idx_event_id",
		"idx_trace_id",
		"idx_span_id",
		"idx_field_names",
		"idx_raw_text",
		"idx_visibility_seq":
		return true
	default:
		return false
	}
}

func addInspectionPhysicalString(
	total *uint64,
	value string,
	allowEmpty bool,
) bool {
	if total == nil ||
		(!allowEmpty && value == "") ||
		len(value) > maximumInspectionPhysicalMetadataBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	valueBytes := uint64(len(value))
	if valueBytes >
		maximumInspectionPhysicalTotalStringBytes-*total {
		return false
	}
	*total += valueBytes
	return true
}

func invalidInspectionResult(message string) error {
	if message == "" {
		return ErrInspectionFailed
	}
	return fmt.Errorf("%w: result %s", ErrInspectionFailed, message)
}
