package spl

import (
	"errors"
	"testing"
)

func TestParseTimechartDocumentedPluralAliases(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		magnitude uint64
		unit      TimeSpanUnit
		minSpan   bool
	}{
		{name: "microsecond abbreviation", source: `index=main | timechart span=250000usecs count`, magnitude: 250000, unit: TimeSpanUnitMicrosecond},
		{name: "microsecond name", source: `index=main | timechart span=250000microseconds count`, magnitude: 250000, unit: TimeSpanUnitMicrosecond},
		{name: "millisecond abbreviation", source: `index=main | timechart span=250msecs count`, magnitude: 250, unit: TimeSpanUnitMillisecond},
		{name: "millisecond name", source: `index=main | timechart span=250milliseconds count`, magnitude: 250, unit: TimeSpanUnitMillisecond},
		{name: "centisecond abbreviation", source: `index=main | timechart span=25csecs count`, magnitude: 25, unit: TimeSpanUnitCentisecond},
		{name: "centisecond name", source: `index=main | timechart span=25centiseconds count`, magnitude: 25, unit: TimeSpanUnitCentisecond},
		{name: "decisecond abbreviation", source: `index=main | timechart minspan=2dsecs count`, magnitude: 2, unit: TimeSpanUnitDecisecond, minSpan: true},
		{name: "decisecond name", source: `index=main | timechart span=2deciseconds count`, magnitude: 2, unit: TimeSpanUnitDecisecond},
		{name: "quarter abbreviation", source: `index=main | timechart span=2qtrs count`, magnitude: 2, unit: TimeSpanUnitQuarter},
		{name: "quarter name", source: `index=main | timechart span=2quarters count`, magnitude: 2, unit: TimeSpanUnitQuarter},
		{name: "year abbreviation", source: `index=main | timechart span=2yrs count`, magnitude: 2, unit: TimeSpanUnitYear},
		{name: "year name", source: `index=main | timechart span=2years count`, magnitude: 2, unit: TimeSpanUnitYear},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command := query.Commands[0].(*TimechartCommand)
			span := command.Span
			if test.minSpan {
				if !command.Axis.MinSpanSpecified {
					t.Fatal("minspan was not preserved")
				}
				span = command.Axis.MinSpan
			}
			if span.Magnitude != test.magnitude || span.Unit != test.unit {
				t.Fatalf("span = %#v, want magnitude %d and unit %s", span, test.magnitude, test.unit)
			}
		})
	}
}

func TestParseTimechartPluralSubsecondAliasesPreserveDivisorValidation(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		spanText string
	}{
		{name: "centiseconds", source: `index=main | timechart span=3csecs count`, spanText: "3csecs"},
		{name: "deciseconds", source: `index=main | timechart minspan=3dsecs count`, spanText: "3dsecs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_INVALID_ARGUMENT" {
				t.Fatalf("Parse error = %#v, want SPL_INVALID_ARGUMENT", err)
			}
			if diagnostic.Message != "timechart span must be a positive integer followed by s, m, or h" {
				t.Fatalf("diagnostic message = %q", diagnostic.Message)
			}
			got := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
			if got != test.spanText {
				t.Fatalf("diagnostic source = %q, want %q", got, test.spanText)
			}
		})
	}
}
