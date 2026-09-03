package plan

import (
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildTimechartCommand(
	result *Query,
	query *spl.Query,
	commandIndex int,
	command *spl.TimechartCommand,
	outputSchemaKnown bool,
	canonicalTimeAvailable bool,
	earliest time.Time,
	latest time.Time,
	searchLocation *time.Location,
) error {
	if command == nil {
		return &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "timechart command is nil",
		}
	}
	if commandIndex+1 != len(query.Commands) {
		next := query.Commands[commandIndex+1]
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_PIPELINE",
			Message:     "timechart must be the final pipeline command",
			Range:       next.SourceRange(),
			Suggestions: []string{"move timechart to the final pipeline stage"},
		}
	}
	measure, measureErr := buildTimechartMeasure(command, outputSchemaKnown)
	if measureErr != nil {
		return measureErr
	}
	if !canonicalTimeAvailable {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
			Message:     "timechart requires the unmodified canonical _time field",
			Range:       command.Range,
			Suggestions: []string{"run timechart before removing, replacing, or transforming _time"},
		}
	}
	span, calendar, spanErr := timechartSpan(command.Span)
	if spanErr != nil {
		return spanErr
	}
	firstBucket, bucketCount, bucketErr := timechartBuckets(
		earliest,
		latest,
		span,
		calendar,
		searchLocation,
		command.Span.Range,
	)
	if bucketErr != nil {
		return bucketErr
	}
	timeField, timeErr := ResolveField("_time", command.Range)
	if timeErr != nil {
		return timeErr
	}
	var split *TimechartSplit
	if command.SplitBy != nil {
		if command.SplitBy.Quoted {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE",
				Message: "timechart split fields must use unquoted exact-field syntax",
				Range:   command.SplitBy.Range,
			}
		}
		resolved, splitErr := ResolveField(
			command.SplitBy.Name,
			command.SplitBy.Range,
		)
		if splitErr != nil {
			return splitErr
		}
		if measure.Input.Name != "" && resolved.Name == measure.Input.Name {
			return &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("timechart aggregate input and split field %q are repeated", resolved.Name),
				Range:   command.SplitBy.Range,
				Suggestions: []string{
					"use a different split field or copy the aggregate input before timechart",
				},
			}
		}
		split, splitErr = buildTimechartSplit(resolved, command.Options, command.Range)
		if splitErr != nil {
			return splitErr
		}
		result.OutputFields = nil
		result.DynamicOutput = &DynamicSeriesOutput{
			FixedFields: []string{"_time"},
			MaxSeries:   timechartMaxSeries(split),
		}
	} else if command.Options != (spl.TimechartOptions{}) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: "timechart limit, useother, and usenull require a BY split field",
			Range:   command.Range,
		}
	} else {
		result.OutputFields = []string{"_time", measure.Output}
		result.DynamicOutput = nil
	}
	result.Operators = append(result.Operators, &Timechart{
		Time:           timeField,
		Split:          split,
		Measure:        measure,
		Span:           span,
		Calendar:       calendar,
		FirstBucket:    firstBucket,
		BucketCount:    bucketCount,
		FixedRange:     true,
		Continuous:     true,
		IncludePartial: true,
		Range:          command.Range,
	})
	return nil
}
