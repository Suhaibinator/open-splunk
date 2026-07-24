package plan

import (
	"reflect"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	timelinePipelineDiagnosticCode = "SPL_UNSUPPORTED_TIMELINE_PIPELINE"
	timelineTimeDiagnosticCode     = "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD"
)

// ValidateTimelineEligibility proves that query still produces identifiable
// source events with their original, visible canonical _time. Timeline counts
// the final relation, so row-selecting Sort, Deduplicate, and Limit operators
// remain eligible and must be preserved by the backend compiler.
//
// The proof is intentionally fail-closed. A future logical operator must gain
// an explicit event-provenance rule here before timelines may consume it.
func ValidateTimelineEligibility(query *Query) error {
	if query == nil || len(query.Operators) == 0 {
		return timelinePipelineDiagnostic(spl.Range{})
	}
	if query.DynamicOutput != nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}
	if scan, ok := query.Operators[0].(*Scan); !ok || scan == nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}

	for index, operator := range query.Operators {
		if operator == nil || isNilOperator(operator) {
			return timelinePipelineDiagnostic(operatorRange(operator))
		}
		switch operator := operator.(type) {
		case *Scan:
			if index != 0 {
				return timelinePipelineDiagnostic(operator.Range)
			}
		case *Filter, *Sort, *Deduplicate, *Limit:
			// These retain event identity and canonical-time provenance.
		case *Extend:
			for _, assignment := range operator.Assignments {
				if assignment.Output.Name == "_time" {
					return timelineTimeDiagnostic(assignment.Range)
				}
			}
		case *Extract:
			for _, capture := range operator.Captures {
				if capture.Output.Name == "_time" {
					return timelineTimeDiagnostic(operator.Range)
				}
			}
		case *Rename:
			for _, assignment := range operator.Assignments {
				if assignment.Source.Name == "_time" || assignment.Destination.Name == "_time" {
					return timelineTimeDiagnostic(assignment.Range)
				}
			}
		case *TimeBucket:
			if operator.Field.Name != "_time" {
				return timelinePipelineDiagnostic(operator.Range)
			}
			return timelineTimeDiagnostic(operator.Range)
		case *Project:
			switch operator.Mode {
			case ProjectModeInclude:
				// The event compiler retains canonical _time implicitly.
			case ProjectModeExclude:
				if containsTimelineTime(operator.Fields) {
					return timelineTimeDiagnostic(operator.Range)
				}
			case ProjectModeTable:
				if !containsTimelineTime(operator.Fields) {
					return timelineTimeDiagnostic(operator.Range)
				}
			default:
				return timelinePipelineDiagnostic(operator.Range)
			}
		case *Aggregate, *Timechart, *Window:
			return timelinePipelineDiagnostic(operator.SourceRange())
		default:
			return timelinePipelineDiagnostic(operator.SourceRange())
		}
	}
	return nil
}

func containsTimelineTime(fields []FieldRef) bool {
	return slices.ContainsFunc(fields, func(field FieldRef) bool { return field.Name == "_time" })
}

func isNilOperator(operator Operator) bool {
	value := reflect.ValueOf(operator)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func operatorRange(operator Operator) spl.Range {
	if operator == nil || isNilOperator(operator) {
		return spl.Range{}
	}
	return operator.SourceRange()
}

func timelinePipelineDiagnostic(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code:    timelinePipelineDiagnosticCode,
		Message: "timeline requires event results; this command transforms or generates rows",
		Range:   sourceRange,
	}
}

func timelineTimeDiagnostic(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code:        timelineTimeDiagnosticCode,
		Message:     "timeline requires the unmodified canonical _time field",
		Range:       sourceRange,
		Suggestions: []string{"request the timeline before removing, replacing, or renaming _time"},
	}
}
