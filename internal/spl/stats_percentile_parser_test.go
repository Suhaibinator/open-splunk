package spl

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseStatsPercentileFamilyCanonicalizesNames(t *testing.T) {
	t.Parallel()

	query, err := Parse(
		`index=main | stats P1(latency) P050(latency) p90(latency) ` +
			`P95(latency) p99(latency) PeRc42(latency) AS answer BY service`,
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 6 {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	wantPercentiles := []uint8{1, 50, 90, 95, 99, 42}
	wantAliases := []string{
		"perc1(latency)",
		"perc50(latency)",
		"perc90(latency)",
		"perc95(latency)",
		"perc99(latency)",
		"answer",
	}
	for index, aggregate := range command.Aggregates {
		if aggregate.Function != AggregateFunctionPercentile ||
			aggregate.Percentile != wantPercentiles[index] ||
			aggregate.Input != "latency" ||
			aggregate.Alias != wantAliases[index] {
			t.Errorf(
				"aggregate[%d] = %#v, want percentile %d alias %q",
				index,
				aggregate,
				wantPercentiles[index],
				wantAliases[index],
			)
		}
	}
	if got := []string{command.GroupBy[0].Name}; !reflect.DeepEqual(got, []string{"service"}) {
		t.Fatalf("group fields = %v", got)
	}
}

func TestParseStatsPercentileRejectsOutOfRangeAndNonSuffixForms(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | stats p0(latency)`,
		`index=main | stats p100(latency)`,
		`index=main | stats perc0(latency)`,
		`index=main | stats perc100(latency)`,
		`index=main | stats p(latency)`,
		`index=main | stats perc(latency)`,
		`index=main | stats percentile95(latency)`,
		`index=main | stats perc(latency, 95)`,
		`index=main | stats perc99.5(latency)`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("Parse error = %T, want *Diagnostic", err)
			}
			if diagnostic.Code != "SPL_UNSUPPORTED_STATS_AGGREGATE" &&
				diagnostic.Code != "SPL_UNSUPPORTED_STATS_SYNTAX" &&
				diagnostic.Code != "SPL_EXPECTED_RIGHT_PAREN" {
				t.Fatalf("diagnostic = %#v, want explicit unsupported percentile syntax", diagnostic)
			}
		})
	}
}
