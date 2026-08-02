package spl

import "testing"

func TestClassifyResultShapeTreatsEventStatsFieldAggregatesAsRowPreserving(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{"minimum", "index=main | eventstats min(latency_ms) AS minimum_latency BY service"},
		{"sum", "index=main | eventstats sum(bytes) AS total_bytes BY level"},
		{"average", "index=main | eventstats avg(duration_ms) AS mean_ms BY service"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := ClassifyResultShape(query); got != (ResultShape{
				Kind: ResultKindEvents,
			}) {
				t.Fatalf("ClassifyResultShape = %#v, want event results", got)
			}
		})
	}
}
