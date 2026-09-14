package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildChartCommand(
	result *Query,
	query *spl.Query,
	commandIndex int,
	command *spl.ChartCommand,
	outputSchemaKnown bool,
) error {
	if commandIndex+1 != len(query.Commands) {
		next := query.Commands[commandIndex+1]
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_CHART_PIPELINE",
			Message:     "chart must be the final pipeline command",
			Range:       next.SourceRange(),
			Suggestions: []string{"move chart to the final pipeline stage"},
		}
	}
	measure, measureErr := buildChartMeasure(command, outputSchemaKnown)
	if measureErr != nil {
		return measureErr
	}
	// usenull and useother default to true, so NULL and OTHER are
	// always reachable public column names. A field spelled like one
	// of them would collide with a series deterministically.
	axes := []spl.StatsGroupField{command.Over, command.SplitBy}
	if command.SingleSplit() {
		axes = axes[:1]
	}
	for _, axis := range axes {
		if axis.Quoted {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "chart axes must use unquoted exact-field syntax",
				Range:   axis.Range,
			}
		}
		if axis.Name == "NULL" || axis.Name == "OTHER" {
			return &Diagnostic{
				Code:        "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message:     fmt.Sprintf("%q and %q are reserved chart series names", "NULL", "OTHER"),
				Range:       axis.Range,
				Suggestions: []string{"rename the field before chart"},
			}
		}
		if !outputSchemaKnown && axis.Name == "fields" {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "chart cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   axis.Range,
				Suggestions: []string{
					"select an exact ordinary field with table before chart",
					"produce a closed stats schema before charting fields",
				},
			}
		}
	}
	over, overErr := ResolveField(command.Over.Name, command.Over.Range)
	if overErr != nil {
		return overErr
	}
	if measure.Function != AggregateFunctionCountRows &&
		measure.Input.Name == over.Name {
		return &Diagnostic{
			Code:    "SPL_DUPLICATE_FIELD",
			Message: fmt.Sprintf("chart aggregate input and row field %q are repeated", over.Name),
			Range:   command.Over.Range,
		}
	}
	if command.SingleSplit() {
		// One split field is Splunk's stats BY table: the row split
		// becomes the group key and the aggregate the only other
		// column, so it plans exactly as `stats <aggregate> BY <row>`.
		statsOptions, optionsErr := buildStatsOptions(spl.StatsOptions{}, command.Range)
		if optionsErr != nil {
			return optionsErr
		}
		result.OutputFields = []string{over.Name, measure.Output}
		result.DynamicOutput = nil
		result.Operators = append(result.Operators, &Aggregate{
			GroupBy:      []FieldRef{over},
			Measures:     []AggregateMeasure{measure},
			StatsOptions: statsOptions,
			Range:        command.Range,
		})
		return nil
	}
	splitBy, splitErr := ResolveField(command.SplitBy.Name, command.SplitBy.Range)
	if splitErr != nil {
		return splitErr
	}
	if over.Name == splitBy.Name {
		return &Diagnostic{
			Code:    "SPL_DUPLICATE_FIELD",
			Message: fmt.Sprintf("chart row and column field %q is repeated", splitBy.Name),
			Range:   command.SplitBy.Range,
		}
	}
	result.OutputFields = nil
	result.DynamicOutput = &DynamicSeriesOutput{
		FixedFields: []string{over.Name},
		MaxSeries:   maxChartSeries,
	}
	result.Operators = append(result.Operators, &Chart{
		Over:         over,
		SplitBy:      splitBy,
		Measure:      measure,
		RowLimit:     maxChartRows,
		SeriesLimit:  chartSeriesLimit,
		IncludeNull:  true,
		IncludeOther: true,
		NullLabel:    "NULL",
		OtherLabel:   "OTHER",
		Range:        command.Range,
	})
	return nil
}
