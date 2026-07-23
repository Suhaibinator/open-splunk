package plan

import "github.com/Suhaibinator/open-splunk/internal/spl"

const fieldAnalysisPipelineDiagnosticCode = "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE"

// ValidateFieldAnalysisEligibility proves that a query's final rows still
// represent individual source events. Field analysis observes the final event
// relation, so projection, computed fields, renames, ordering, deduplication,
// and limits remain semantically significant and must be preserved.
//
// The proof is deliberately fail-closed: every future logical operator must
// gain an explicit event-provenance rule before field analysis may consume it.
func ValidateFieldAnalysisEligibility(query *Query) error {
	if query == nil || len(query.Operators) == 0 || query.DynamicOutput != nil {
		return fieldAnalysisPipelineDiagnostic(firstFieldAnalysisRange(query))
	}
	if scan, ok := query.Operators[0].(*Scan); !ok || scan == nil {
		return fieldAnalysisPipelineDiagnostic(operatorRange(query.Operators[0]))
	}

	for index, operator := range query.Operators {
		if operator == nil || isNilOperator(operator) {
			return fieldAnalysisPipelineDiagnostic(operatorRange(operator))
		}
		switch operator := operator.(type) {
		case *Scan:
			if index != 0 {
				return fieldAnalysisPipelineDiagnostic(operator.Range)
			}
		case *Filter, *Extend, *Rename, *Sort, *Deduplicate, *Limit:
			// These preserve source-event identity while changing the final
			// relation, schema, values, or order consumed by field analysis.
		case *Project:
			switch operator.Mode {
			case ProjectModeInclude, ProjectModeExclude, ProjectModeTable:
				// Every supported projection still represents source events.
			default:
				return fieldAnalysisPipelineDiagnostic(operator.Range)
			}
		case *Aggregate, *Timechart, *Window:
			return fieldAnalysisPipelineDiagnostic(operator.SourceRange())
		default:
			return fieldAnalysisPipelineDiagnostic(operator.SourceRange())
		}
	}
	return nil
}

func firstFieldAnalysisRange(query *Query) spl.Range {
	if query == nil || len(query.Operators) == 0 {
		return spl.Range{}
	}
	return operatorRange(query.Operators[0])
}

func fieldAnalysisPipelineDiagnostic(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code:    fieldAnalysisPipelineDiagnosticCode,
		Message: "field analysis requires event results; this command transforms or generates rows",
		Range:   sourceRange,
	}
}
