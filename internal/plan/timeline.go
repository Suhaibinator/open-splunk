package plan

import (
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
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
	if err := ValidateKnowledgePreludeIntegrity(query); err != nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}
	if err := ValidateAutomaticLookupIntegrity(query); err != nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}
	if query.DynamicOutput != nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}
	if scan, ok := query.Operators[0].(*Scan); !ok || scan == nil {
		return timelinePipelineDiagnostic(operatorRange(query.Operators[0]))
	}

	for index, operator := range query.Operators {
		if nilcheck.IsNil(operator) {
			return timelinePipelineDiagnostic(operatorRange(operator))
		}
		switch operator := operator.(type) {
		case *Scan:
			if index != 0 {
				return timelinePipelineDiagnostic(operator.Range)
			}
		case *Filter, *RegexFilter, *Sort, *Deduplicate, *Limit, *Reverse:
			// These retain event identity and canonical-time provenance.
		case *EventAggregate:
			if !validEventAggregateContract(operator) {
				return timelinePipelineDiagnostic(operator.Range)
			}
			if operator.Measure.Output == "_time" {
				return timelineTimeDiagnostic(operator.Range)
			}
		case *StreamAggregate:
			if !validStreamAggregateContract(operator) {
				return timelinePipelineDiagnostic(operator.Range)
			}
			if operator.Measure.Output == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.OutputRange, operator.Range))
			}
		case *Extend:
			for _, assignment := range operator.Assignments {
				if assignment.Output.Name == "_time" {
					return timelineTimeDiagnostic(assignment.Range)
				}
			}
		case *Strcat:
			if operator.Destination.Name == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.Destination.Range, operator.Range))
			}
		case *FillNull:
			for _, field := range operator.Fields {
				if field.Name == "_time" {
					return timelineTimeDiagnostic(preferTimelineRange(field.Range, operator.Range))
				}
			}
		case *RowTotal:
			if operator.Output == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.OutputRange, operator.Range))
			}
		case *OrderedDelta:
			if operator.Output == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.OutputRange, operator.Range))
			}
		case *MakeMultivalue:
			// makemv replaces its input in place. An authored command cannot
			// target compiler-private fields, but keep the provenance proof safe
			// for directly constructed or future parser-produced plans.
			if operator.Input.Name == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.Input.Range, operator.Range))
			}
		case *NoMultivalue:
			if operator.Input.Name == "_time" {
				return timelineTimeDiagnostic(preferTimelineRange(operator.Input.Range, operator.Range))
			}
		case *Extract:
			for _, capture := range operator.Captures {
				if capture.Output.Name == "_time" {
					return timelineTimeDiagnostic(operator.Range)
				}
			}
		case *ExtractJSON:
			if operator.Output.Name == "_time" {
				return timelineTimeDiagnostic(operator.Range)
			}
		case *Lookup:
			if !validLookupContract(operator) {
				return timelinePipelineDiagnostic(operator.Range)
			}
			if operator.WriteMode == LookupWriteModeOverwrite {
				for _, output := range operator.Outputs {
					if output.EventField.Name == "_time" {
						return timelineTimeDiagnostic(output.Range)
					}
				}
			}
		case *AutomaticLookupGroup:
			if !validAutomaticLookupGroup(operator) {
				return timelinePipelineDiagnostic(operator.SourceRange())
			}
			for _, automatic := range operator.entries {
				if automatic.lookup.WriteMode != LookupWriteModeOverwrite {
					continue
				}
				for _, output := range automatic.lookup.Outputs {
					if output.EventField.Name == "_time" {
						return timelineTimeDiagnostic(output.Range)
					}
				}
			}
		case *ConditionalExtract:
			for _, capture := range operator.Extraction().Captures() {
				if capture.Name() == "_time" {
					return timelineTimeDiagnostic(operator.SourceRange())
				}
			}
		case *ConditionalExtractJSON:
			if operator.Extraction().Output() == "_time" {
				return timelineTimeDiagnostic(operator.SourceRange())
			}
		case *CopyFieldAlias:
			for _, assignment := range operator.Assignments() {
				if assignment.Destination() == "_time" {
					return timelineTimeDiagnostic(operator.SourceRange())
				}
			}
		case *ParallelExtend:
			for _, assignment := range operator.Assignments() {
				if assignment.Destination() == "_time" {
					return timelineTimeDiagnostic(operator.SourceRange())
				}
			}
		case *Rename:
			for _, assignment := range operator.Assignments {
				if assignment.Source.Name == "_time" || assignment.Destination.Name == "_time" {
					return timelineTimeDiagnostic(assignment.Range)
				}
			}
		case *TimeBucket:
			if operator.Field.Name != "_time" || operator.Output.Name == "" {
				return timelinePipelineDiagnostic(operator.Range)
			}
			if operator.Output.Name == "_time" {
				return timelineTimeDiagnostic(operator.Range)
			}
		case *NumericBucket:
			if operator.Input.Name == "" || operator.Output.Name == "" || operator.Span == 0 {
				return timelinePipelineDiagnostic(operator.Range)
			}
			if operator.Output.Name == "_time" {
				return timelineTimeDiagnostic(operator.Range)
			}
		case *Project:
			switch operator.Mode {
			case ProjectModeInclude:
				// The event compiler retains canonical _time implicitly.
			case ProjectModeExclude:
				if containsTimelineTime(operator.Fields) ||
					projectPatternsMatch(operator.Patterns, "_time") {
					return timelineTimeDiagnostic(operator.Range)
				}
			case ProjectModeTable:
				if !containsTimelineTime(operator.Fields) {
					return timelineTimeDiagnostic(operator.Range)
				}
			default:
				return timelinePipelineDiagnostic(operator.Range)
			}
		// Aggregate, Timechart, Chart, Window and ExpandMultivalue break
		// source-event identity and fall through with every unknown operator.
		default:
			return timelinePipelineDiagnostic(operator.SourceRange())
		}
	}
	return nil
}

func projectPatternsMatch(patterns []ProjectFieldPattern, name string) bool {
	return slices.ContainsFunc(patterns, func(pattern ProjectFieldPattern) bool {
		return spl.MatchFieldsFieldGlob(pattern.Pattern, name)
	})
}

func preferTimelineRange(preferred, fallback spl.Range) spl.Range {
	if preferred != (spl.Range{}) {
		return preferred
	}
	return fallback
}

func containsTimelineTime(fields []FieldRef) bool {
	return slices.ContainsFunc(fields, func(field FieldRef) bool { return field.Name == "_time" })
}

func operatorRange(operator Operator) spl.Range {
	if nilcheck.IsNil(operator) {
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
