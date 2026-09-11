package spl

import (
	"errors"
	"testing"
)

func TestParseTimechartAcceptsAutomaticSpanAcrossAggregates(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | timechart count`,
		`index=main | timechart count by message`,
		`index=main | timechart count(status)`,
		`index=main | timechart p95(latency)`,
		`index=main | timechart sum(bytes) by service`,
		`index=main | timechart avg(latency) AS mean`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command := query.Commands[0].(*TimechartCommand)
			if command.Span != (TimeSpan{}) || command.Axis != (TimechartAxisOptions{}) {
				t.Fatalf("automatic timechart metadata = %#v", command)
			}
		})
	}
}

func TestParseTimechartAcceptsLeadingAutomaticAxisOptions(t *testing.T) {
	t.Parallel()

	source := `index=main | timechart bins=37 minspan=2d count BY message`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*TimechartCommand)
	if !command.Axis.BinsSpecified || command.Axis.Bins != 37 ||
		!command.Axis.MinSpanSpecified || command.Axis.MinSpan.Magnitude != 2 ||
		command.Axis.MinSpan.Unit != TimeSpanUnitDay {
		t.Fatalf("axis options = %#v", command.Axis)
	}
}

func TestParseTimechartExplicitSpanCanAccompanyAutomaticOptions(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | timechart bins=1 minspan=2d span=1m count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*TimechartCommand)
	if command.Span.Magnitude != 1 || command.Span.Unit != TimeSpanUnitMinute ||
		!command.Axis.BinsSpecified || !command.Axis.MinSpanSpecified {
		t.Fatalf("explicit timechart metadata = %#v", command)
	}
}

func TestParseTimechartRejectsInvalidOrTrailingAxisOptions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | timechart bins=0 count`, "SPL_UNSUPPORTED_TIMECHART_BINS"},
		{`index=main | timechart bins=10001 count`, "SPL_UNSUPPORTED_TIMECHART_BINS"},
		{`index=main | timechart bins=no count`, "SPL_INVALID_ARGUMENT"},
		{`index=main | timechart minspan=0m count`, "SPL_INVALID_ARGUMENT"},
		{`index=main | timechart count BY message bins=10`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{`index=main | timechart count BY message minspan=1m`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
		{`index=main | timechart count BY message span=1m`, "SPL_UNSUPPORTED_TIMECHART_SYNTAX"},
	} {
		t.Run(test.source, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse error = %#v, want %s", err, test.code)
			}
		})
	}
}

func TestParseTimechartMonthAliasesAreCanonicalized(t *testing.T) {
	t.Parallel()

	for _, authored := range []string{"1mon", "1month"} {
		query, err := Parse(`index=main | timechart span=` + authored + ` count`)
		if err != nil {
			t.Fatalf("Parse(%s): %v", authored, err)
		}
		span := query.Commands[0].(*TimechartCommand).Span
		if span.Magnitude != 1 || span.Unit != TimeSpanUnitMonth || span.Unit.String() != "month" {
			t.Fatalf("Parse(%s) span = %#v", authored, span)
		}
	}
}
