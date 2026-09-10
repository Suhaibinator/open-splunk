package plan

import (
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildTimechartCommand(
	result *Query,
	_ *spl.Query,
	_ int,
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
	measure, measureErr := buildTimechartMeasure(command, outputSchemaKnown)
	if measureErr != nil {
		return measureErr
	}
	if !canonicalTimeAvailable && !outputSchemaKnown {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
			Message:     "timechart requires the unmodified canonical _time field",
			Range:       command.Range,
			Suggestions: []string{"run timechart before removing, replacing, or transforming _time"},
		}
	}
	op := &Timechart{AuthoredSpan: command.Span, Axis: command.Axis, SearchEarliest: earliest, SearchLatest: latest, FixedRange: true, Continuous: true, IncludePartial: true, Range: command.Range}
	if command.Axis.ContSpecified {
		op.Continuous = command.Axis.Cont
	}
	if command.Axis.PartialSpecified {
		op.IncludePartial = command.Axis.Partial
	}
	if command.Axis.FixedRangeSpecified {
		op.FixedRange = command.Axis.FixedRange
	}
	alignment, alignmentErr := resolveTimechartAlignment(command.Axis, earliest, latest, result.SearchStart, searchLocation)
	if alignmentErr != nil {
		return &Diagnostic{Code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX", Message: alignmentErr.Error(), Range: command.Axis.AlignTimeRange}
	}
	op.Alignment = alignment
	if op.FixedRange {
		if err := ResolveTimechartGrid(op, earliest, latest, result.SearchTimezone); err != nil {
			return err
		}
	} else {
		// Span selection is deferred until the post-filter source is materialized.
		if _, err := validateTimechartAxisOptions(command.Axis, command.Range); err != nil {
			return err
		}
		if command.Span != (spl.TimeSpan{}) {
			if _, _, _, err := enhancedTimechartSpan(command.Span); err != nil {
				return err
			}
		}
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
	op.Time, op.Split, op.Measure = timeField, split, measure
	result.Operators = append(result.Operators, op)
	return nil
}
