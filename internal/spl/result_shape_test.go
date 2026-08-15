package spl

import "testing"

func TestClassifyResultShapeAppliesTransformationsInPipelineOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source      string
		wantKind    ResultKind
		wantRuntime bool
	}{
		{name: "events", source: "index=main", wantKind: ResultKindEvents},
		{name: "row preserving", source: `index=main | rex "(?<id>id=\w+)"`, wantKind: ResultKindEvents},
		{name: "eventstats preserves events", source: "index=main | eventstats count BY level", wantKind: ResultKindEvents},
		{name: "table", source: "index=main | table level count", wantKind: ResultKindStatistics},
		{name: "stats", source: "index=main | stats count BY level", wantKind: ResultKindStatistics},
		{name: "top", source: "index=main | top limit=20 message", wantKind: ResultKindStatistics},
		{name: "rare", source: "index=main | rare limit=20 message", wantKind: ResultKindStatistics},
		{
			name:        "timechart",
			source:      "index=main | timechart span=5m count BY level",
			wantKind:    ResultKindTimeSeries,
			wantRuntime: true,
		},
		{
			name:     "unsplit timechart",
			source:   "index=main | timechart span=5m count",
			wantKind: ResultKindTimeSeries,
		},
		{
			name:        "chart",
			source:      "index=main | chart count OVER path BY level",
			wantKind:    ResultKindStatistics,
			wantRuntime: true,
		},
		{
			name:        "later timechart replaces table",
			source:      "index=main | table _time level | timechart span=5m count BY level",
			wantKind:    ResultKindTimeSeries,
			wantRuntime: true,
		},
		{
			name:     "later table replaces timechart",
			source:   "index=main | timechart span=5m count BY level | table _time level",
			wantKind: ResultKindStatistics,
		},
		{
			name:        "row preserving command retains timechart",
			source:      "index=main | timechart span=5m count BY level | head 10",
			wantKind:    ResultKindTimeSeries,
			wantRuntime: true,
		},
		{
			name:        "eventstats retains timechart shape",
			source:      "index=main | timechart span=5m count BY level | eventstats count BY _time",
			wantKind:    ResultKindTimeSeries,
			wantRuntime: true,
		},
		{
			name:        "later chart replaces stats",
			source:      "index=main | stats count BY path level | chart count OVER path BY level",
			wantKind:    ResultKindStatistics,
			wantRuntime: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse(%q): %v", test.source, err)
			}
			got := ClassifyResultShape(query)
			if got.Kind != test.wantKind || got.RuntimeNamedColumns != test.wantRuntime {
				t.Fatalf("ClassifyResultShape() = %+v, want kind=%v runtime=%t", got, test.wantKind, test.wantRuntime)
			}
		})
	}
}

func TestClassifyResultShapeFailsClosedForMalformedAST(t *testing.T) {
	t.Parallel()

	var typedNil *TableCommand
	for _, query := range []*Query{
		nil,
		{Commands: []Command{nil}},
		{Commands: []Command{typedNil}},
	} {
		if got := ClassifyResultShape(query); got != (ResultShape{}) {
			t.Fatalf("ClassifyResultShape(%+v) = %+v, want invalid", query, got)
		}
	}
}
